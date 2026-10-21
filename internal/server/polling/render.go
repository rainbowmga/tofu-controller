package polling

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/http/fetch"
	"github.com/fluxcd/pkg/kustomize"
	securefs "github.com/fluxcd/pkg/kustomize/filesys"
	"github.com/fluxcd/pkg/tar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
)

const (
	kustomizeNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	kustomizeNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"

	// renderSourceLabel marks the per pull request sources used to render
	// Kustomizations; its value identifies the repository.
	renderSourceLabel = "infra.weave.works/render-source"
)

var kustomizationGVK = schema.GroupVersionKind{
	Group:   "kustomize.toolkit.fluxcd.io",
	Version: "v1",
	Kind:    "Kustomization",
}

// errRenderPending means the pull request's render source has not fetched the
// revision the pull request is planned from yet.
var errRenderPending = errors.New("render source is not at the planned revision yet")

// planRef is where a pull request is planned from: its head branch. The
// provider's merge of the pull request onto its base (refs/pull/N/merge)
// would show what merging applies, but source-controller cannot fetch it:
// for a ref that is neither a branch nor a tag, fluxcd/pkg/git clones the
// branches and then looks the commit up, and a merge commit is on none of
// them. Until that is fixed upstream the head is planned, and the plan of a
// branch behind its base also shows reverting what the base gained.
func planRef(pr provider.PullRequest) sourcev1.GitRepositoryRef {
	return sourcev1.GitRepositoryRef{Branch: pr.HeadBranch}
}

func planSha(pr provider.PullRequest) string {
	return pr.HeadSha
}

// prRender is a Terraform object as a pull request would apply it, rendered
// from the pull request revision of the Flux Kustomization that applies it.
// changed reports whether what it plans differs from the same render at the
// revision the Kustomization currently applies.
type prRender struct {
	terraform *infrav1.Terraform
	changed   bool
}

// renderCache keeps renders per Kustomization and revision, so the stacks one
// Kustomization applies share a single download and build.
type renderCache struct {
	mu      sync.Mutex
	entries map[string]renderEntry
}

type renderEntry struct {
	terraforms []*infrav1.Terraform
}

func (e renderEntry) find(name, namespace string) *infrav1.Terraform {
	for _, tf := range e.terraforms {
		if tf.Name == name && (tf.Namespace == namespace || tf.Namespace == "") {
			return tf
		}
	}
	return nil
}

func (c *renderCache) get(key string) (renderEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return e, ok
}

func (c *renderCache) put(key string, e renderEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) > 256 {
		c.entries = map[string]renderEntry{}
	}
	c.entries[key] = e
}

// renderForPullRequest renders original as the pull request would apply it.
// It returns nil when original was not applied by a Flux Kustomization, or by
// one whose source is a different repository; the caller then plans the
// in-cluster object as before.
func (s *Server) renderForPullRequest(ctx context.Context, original *infrav1.Terraform, source *sourcev1.GitRepository, pr provider.PullRequest, prID string) (*prRender, error) {
	ksName := original.Labels[kustomizeNameLabel]
	if ksName == "" {
		return nil, nil
	}
	ksNamespace := original.Labels[kustomizeNamespaceLabel]
	if ksNamespace == "" {
		ksNamespace = original.Namespace
	}

	ks := &unstructured.Unstructured{}
	ks.SetGroupVersionKind(kustomizationGVK)
	if err := s.clusterClient.Get(ctx, client.ObjectKey{Namespace: ksNamespace, Name: ksName}, ks); err != nil {
		return nil, fmt.Errorf("unable to get Kustomization %s/%s: %w", ksNamespace, ksName, err)
	}

	ksSource, err := s.kustomizationSource(ctx, ks)
	if err != nil || ksSource == nil {
		return nil, err
	}
	ksRepo, err := repoKey(ksSource.Spec.URL)
	if err != nil {
		return nil, nil
	}
	if repo, err := repoKey(source.Spec.URL); err != nil || repo != ksRepo {
		return nil, nil
	}

	renderSource, err := s.reconcileRenderSource(ctx, ksSource, ksRepo, planRef(pr), prID)
	if err != nil {
		return nil, err
	}
	artifact := renderSource.GetArtifact()
	if artifact == nil || !strings.HasSuffix(artifact.Revision, planSha(pr)) {
		if err := s.requestReconcile(ctx, renderSource, planSha(pr)); err != nil {
			return nil, err
		}
		return nil, errRenderPending
	}

	head, err := s.renderRevision(ctx, ks, artifact)
	if err != nil {
		return nil, err
	}
	headTF := head.find(original.Name, original.Namespace)
	if headTF == nil {
		// The pull request removes this stack, so there is nothing to plan.
		return &prRender{changed: true}, nil
	}

	// The stack in the cluster is what the Kustomization rendered from its own
	// artifact, so that render is the baseline a pull request is compared to.
	baseArtifact := ksSource.GetArtifact()
	if baseArtifact == nil {
		return nil, fmt.Errorf("source of Kustomization %s/%s has no artifact", ksNamespace, ksName)
	}
	base, err := s.renderRevision(ctx, ks, baseArtifact)
	if err != nil {
		return nil, err
	}
	baseTF := base.find(original.Name, original.Namespace)

	return &prRender{
		terraform: headTF.DeepCopy(),
		changed:   baseTF == nil || !plannedFieldsEqual(baseTF.Spec, headTF.Spec),
	}, nil
}

// renderRevision builds the Kustomization from artifact, once per revision.
func (s *Server) renderRevision(ctx context.Context, ks *unstructured.Unstructured, artifact *meta.Artifact) (renderEntry, error) {
	key := fmt.Sprintf("%s/%s@%s", ks.GetNamespace(), ks.GetName(), artifact.Revision)
	if entry, ok := s.renders.get(key); ok {
		return entry, nil
	}
	entry, err := renderArtifact(ctx, s.clusterClient, *ks, artifact.URL, artifact.Digest)
	if err != nil {
		return renderEntry{}, fmt.Errorf("unable to render Kustomization %s/%s at %s: %w", ks.GetNamespace(), ks.GetName(), artifact.Revision, err)
	}
	s.renders.put(key, entry)
	return entry, nil
}

// plannedFieldsEqual reports whether two specs would plan the same thing,
// comparing only the fields a pull request can change.
func plannedFieldsEqual(a, b infrav1.TerraformSpec) bool {
	var none infrav1.TerraformSpec
	return equality.Semantic.DeepEqual(plannedSpec(none, a), plannedSpec(none, b))
}

// plannedSpec is the spec to plan for a pull request: what the stack plans
// comes from the pull request, while how and as whom the runner runs, which
// Secrets and files it gets, its backend and its webhooks stay as merged. An
// unmerged pull request must not pick the runner's service account or pod,
// or reference Secrets the merged stack does not.
func plannedSpec(original, rendered infrav1.TerraformSpec) infrav1.TerraformSpec {
	spec := *original.DeepCopy()
	spec.Path = rendered.Path
	spec.Vars = rendered.Vars
	spec.Values = rendered.Values
	spec.Targets = rendered.Targets
	spec.Parallelism = rendered.Parallelism
	spec.Destroy = rendered.Destroy
	return spec
}

func (s *Server) kustomizationSource(ctx context.Context, ks *unstructured.Unstructured) (*sourcev1.GitRepository, error) {
	kind, _, _ := unstructured.NestedString(ks.Object, "spec", "sourceRef", "kind")
	if kind != sourcev1.GitRepositoryKind {
		return nil, nil
	}
	name, _, _ := unstructured.NestedString(ks.Object, "spec", "sourceRef", "name")
	namespace, _, _ := unstructured.NestedString(ks.Object, "spec", "sourceRef", "namespace")
	if namespace == "" {
		namespace = ks.GetNamespace()
	}

	src := &sourcev1.GitRepository{}
	if err := s.clusterClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, src); err != nil {
		return nil, fmt.Errorf("unable to get source of Kustomization %s/%s: %w", ks.GetNamespace(), ks.GetName(), err)
	}
	return src, nil
}

// reconcileRenderSource keeps one source per repository and pull request,
// tracking the revision the pull request is planned from, for rendering
// Kustomizations.
func (s *Server) reconcileRenderSource(ctx context.Context, ksSource *sourcev1.GitRepository, repo string, ref sourcev1.GitRepositoryRef, prID string) (*sourcev1.GitRepository, error) {
	src := &sourcev1.GitRepository{
		ObjectMeta: renderSourceMeta(ksSource.Namespace, repo, prID),
	}

	_, err := controllerutil.CreateOrUpdate(ctx, s.clusterClient, src, func() error {
		spec := ksSource.Spec.DeepCopy()
		spec.Reference = &ref
		spec.Interval.Duration = s.branchPollingInterval
		if spec.Interval.Duration == 0 {
			spec.Interval.Duration = time.Minute
		}
		spec.Suspend = false
		src.Spec = *spec
		src.SetLabels(map[string]string{
			bpconfig.LabelKey:     bpconfig.LabelValue,
			bpconfig.LabelPRIDKey: prID,
			renderSourceLabel:     repo,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to reconcile render source %s: %w", src.Name, err)
	}
	return src, nil
}

// requestReconcile asks source-controller to fetch the pull request head now
// instead of at its next interval, once per head revision.
func (s *Server) requestReconcile(ctx context.Context, src *sourcev1.GitRepository, head string) error {
	if src.GetAnnotations()[meta.ReconcileRequestAnnotation] == head {
		return nil
	}
	patch := client.MergeFrom(src.DeepCopy())
	ann := src.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[meta.ReconcileRequestAnnotation] = head
	src.SetAnnotations(ann)
	if err := s.clusterClient.Patch(ctx, src, patch); err != nil {
		return fmt.Errorf("unable to request reconciliation of %s: %w", src.Name, err)
	}
	return nil
}

// deleteStaleRenderSources removes render sources of the repository whose
// pull request is no longer open.
func (s *Server) deleteStaleRenderSources(ctx context.Context, namespace, url string, open map[string]bool) error {
	repo, err := repoKey(url)
	if err != nil {
		return nil
	}
	list := &sourcev1.GitRepositoryList{}
	if err := s.clusterClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		bpconfig.LabelKey: bpconfig.LabelValue,
		renderSourceLabel: repo,
	}); err != nil {
		return fmt.Errorf("unable to list render sources: %w", err)
	}
	for i := range list.Items {
		src := &list.Items[i]
		if open[src.Labels[bpconfig.LabelPRIDKey]] {
			continue
		}
		if err := s.clusterClient.Delete(ctx, src); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("unable to delete render source %s: %w", src.Name, err)
		}
	}
	return nil
}

// renderArtifact fetches a source artifact the way kustomize-controller does,
// and renders the Kustomization from it.
func renderArtifact(ctx context.Context, kubeClient client.Client, ks unstructured.Unstructured, url, digest string) (renderEntry, error) {
	root, err := os.MkdirTemp("", "branch-planner-render-")
	if err != nil {
		return renderEntry{}, err
	}
	defer os.RemoveAll(root)

	fetcher := fetch.New(
		fetch.WithRetries(3),
		fetch.WithMaxDownloadSize(tar.UnlimitedUntarSize),
		fetch.WithUntar(tar.WithMaxUntarSize(tar.UnlimitedUntarSize)),
		fetch.WithHostnameOverwrite(os.Getenv("SOURCE_CONTROLLER_LOCALHOST")),
	)
	if err := fetcher.FetchWithContext(ctx, url, digest, root); err != nil {
		return renderEntry{}, fmt.Errorf("unable to fetch artifact: %w", err)
	}
	return renderDir(ctx, kubeClient, ks, root)
}

// renderDir builds the Kustomization the way kustomize-controller does, and
// returns the Terraform objects it produces.
func renderDir(ctx context.Context, kubeClient client.Client, ks unstructured.Unstructured, root string) (renderEntry, error) {
	path, _, _ := unstructured.NestedString(ks.Object, "spec", "path")
	dirPath, err := securejoin.SecureJoin(root, path)
	if err != nil {
		return renderEntry{}, err
	}
	if _, err := os.Stat(dirPath); err != nil {
		return renderEntry{}, fmt.Errorf("kustomization path %q not found: %w", path, err)
	}

	if _, err := kustomize.NewGenerator(root, ks).WriteFile(dirPath); err != nil {
		return renderEntry{}, fmt.Errorf("unable to generate kustomization.yaml: %w", err)
	}

	fs, err := securefs.MakeFsOnDiskSecure(root)
	if err != nil {
		return renderEntry{}, err
	}
	resMap, err := kustomize.Build(fs, dirPath)
	if err != nil {
		return renderEntry{}, fmt.Errorf("kustomize build failed: %w", err)
	}

	_, hasPostBuild, _ := unstructured.NestedMap(ks.Object, "spec", "postBuild")
	strategy, _, _ := unstructured.NestedString(ks.Object, "spec", "postBuild", "substituteStrategy")
	always := strategy == "Always"

	var entry renderEntry
	for _, res := range resMap.Resources() {
		if res.GetKind() != infrav1.TerraformKind || !strings.HasPrefix(res.GetApiVersion(), infrav1.GroupVersion.Group+"/") {
			continue
		}
		if hasPostBuild {
			substituted, err := kustomize.SubstituteVariables(ctx, kubeClient, ks, res, kustomize.SubstituteWithAlways(always))
			if err != nil {
				return renderEntry{}, fmt.Errorf("variable substitution failed for %s: %w", res.GetName(), err)
			}
			if substituted != nil {
				res = substituted
			}
		}

		obj, err := res.Map()
		if err != nil {
			return renderEntry{}, err
		}
		tf := &infrav1.Terraform{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj, tf); err != nil {
			return renderEntry{}, fmt.Errorf("unable to decode Terraform %s: %w", res.GetName(), err)
		}
		entry.terraforms = append(entry.terraforms, tf)
	}
	return entry, nil
}

// repoKey identifies a repository independent of how its URL is written.
func repoKey(url string) (string, error) {
	repo, err := provider.RepoFromURL(url)
	if err != nil {
		return "", err
	}
	return bpconfig.GenerateUniqueHash(strings.ToLower(repo.String())), nil
}

func renderSourceMeta(namespace, repo, prID string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: namespace,
		Name:      bpconfig.PullRequestObjectName("branch-planner-render-"+repo, prID),
	}
}
