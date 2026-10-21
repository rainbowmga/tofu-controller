package polling

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/fluxcd/pkg/kustomize"
	securefs "github.com/fluxcd/pkg/kustomize/filesys"
	"github.com/fluxcd/pkg/tar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
)

const (
	kustomizeNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	kustomizeNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"

	// renderSourceLabel marked the per pull request GitRepositories earlier
	// releases rendered from. Renders now come from the provider's archives,
	// and the planner removes any such sources it still finds.
	renderSourceLabel = "infra.weave.works/render-source"
)

var kustomizationGVK = schema.GroupVersionKind{
	Group:   "kustomize.toolkit.fluxcd.io",
	Version: "v1",
	Kind:    "Kustomization",
}

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
// from the pull request head of the Flux Kustomization that applies it.
// changed reports whether what it plans differs from the same render at the
// commit the pull request branched from, that is, whether the pull request
// itself changes it.
type prRender struct {
	terraform *infrav1.Terraform
	changed   bool
}

// renderCache keeps renders per Kustomization and commit, so the stacks one
// Kustomization applies share a single download and build.
type renderCache = cache[renderEntry]

// cache is a bounded map that forgets everything rather than evict: the
// entries it holds are cheap to recompute and few are live at once.
type cache[T any] struct {
	mu      sync.Mutex
	entries map[string]T
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

func (c *cache[T]) get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	return e, ok
}

func (c *cache[T]) put(key string, e T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) > 256 {
		c.entries = map[string]T{}
	}
	c.entries[key] = e
}

// renderForPullRequest renders original as the pull request would apply it.
// It returns nil when original was not applied by a Flux Kustomization, by
// one whose source is a different repository, or when the provider cannot
// serve archives; the caller then plans the in-cluster object as before.
func (s *Server) renderForPullRequest(ctx context.Context, gitProvider provider.Provider, original *infrav1.Terraform, source *sourcev1.GitRepository, pr provider.PullRequest, prID string) (*prRender, error) {
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

	mergeBase, err := s.mergeBase(ctx, gitProvider, pr)
	if err != nil {
		return nil, err
	}
	if mergeBase == "" {
		return nil, nil
	}

	head, err := s.renderCommit(ctx, gitProvider, pr.Repository, ks, pr.HeadSha)
	if errors.Is(err, provider.ErrArchiveUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	headTF := head.find(original.Name, original.Namespace)
	if headTF == nil {
		// The pull request removes this stack, so there is nothing to plan.
		return &prRender{changed: true}, nil
	}

	// What the pull request changes is the difference to the commit it
	// branched from. Against the base's tip, a branch behind it would look
	// like it changes every stack the base has touched since.
	base, err := s.renderCommit(ctx, gitProvider, pr.Repository, ks, mergeBase)
	if err != nil {
		return nil, err
	}
	baseTF := base.find(original.Name, original.Namespace)

	return &prRender{
		terraform: headTF.DeepCopy(),
		changed:   baseTF == nil || !plannedFieldsEqual(baseTF.Spec, headTF.Spec),
	}, nil
}

// mergeBase is the commit a pull request branched from, asked of the provider
// once per head: every stack of every poll compares against it, and it only
// moves with the head.
func (s *Server) mergeBase(ctx context.Context, gitProvider provider.Provider, pr provider.PullRequest) (string, error) {
	key := fmt.Sprintf("%s#%d@%s", pr.Repository, pr.Number, pr.HeadSha)
	if base, ok := s.mergeBases.get(key); ok {
		return base, nil
	}
	comparison, err := gitProvider.CompareWithBase(ctx, pr)
	if err != nil {
		return "", err
	}
	if comparison.MergeBase != "" {
		s.mergeBases.put(key, comparison.MergeBase)
	}
	return comparison.MergeBase, nil
}

// renderCommit builds the Kustomization from the provider's archive of the
// repository at sha, once per commit.
func (s *Server) renderCommit(ctx context.Context, gitProvider provider.Provider, repo provider.Repository, ks *unstructured.Unstructured, sha string) (renderEntry, error) {
	key := fmt.Sprintf("%s/%s@%s", ks.GetNamespace(), ks.GetName(), sha)
	if entry, ok := s.renders.get(key); ok {
		return entry, nil
	}
	entry, err := renderArchive(ctx, s.clusterClient, gitProvider, repo, *ks, sha)
	if err != nil {
		return renderEntry{}, fmt.Errorf("unable to render Kustomization %s/%s at %s: %w", ks.GetNamespace(), ks.GetName(), sha, err)
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

// deleteRenderSources removes the per pull request render sources earlier
// releases kept, which nothing reads any more.
func (s *Server) deleteRenderSources(ctx context.Context, namespace string) error {
	list := &sourcev1.GitRepositoryList{}
	if err := s.clusterClient.List(ctx, list, client.InNamespace(namespace), client.HasLabels{renderSourceLabel}); err != nil {
		return fmt.Errorf("unable to list render sources: %w", err)
	}
	for i := range list.Items {
		if err := s.clusterClient.Delete(ctx, &list.Items[i]); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("unable to delete render source %s: %w", list.Items[i].Name, err)
		}
	}
	return nil
}

// renderArchive downloads the repository at sha from the provider and renders
// the Kustomization from it. The archive holds one top-level directory.
func renderArchive(ctx context.Context, kubeClient client.Client, gitProvider provider.Provider, repo provider.Repository, ks unstructured.Unstructured, sha string) (renderEntry, error) {
	archive, err := gitProvider.DownloadArchive(ctx, repo, sha)
	if err != nil {
		return renderEntry{}, err
	}
	defer archive.Close()

	root, err := os.MkdirTemp("", "branch-planner-render-")
	if err != nil {
		return renderEntry{}, err
	}
	defer os.RemoveAll(root)

	if err := tar.Untar(archive, root, tar.WithMaxUntarSize(tar.UnlimitedUntarSize)); err != nil {
		return renderEntry{}, fmt.Errorf("unable to extract archive: %w", err)
	}
	top, err := singleDirectory(root)
	if err != nil {
		return renderEntry{}, err
	}
	return renderDir(ctx, kubeClient, ks, top)
}

// singleDirectory is the one directory an archive extracted into root; the
// archive's pax header may sit next to it as a file.
func singleDirectory(root string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	if len(dirs) != 1 {
		return "", fmt.Errorf("archive has %d top-level directories, expected one", len(dirs))
	}
	return filepath.Join(root, dirs[0]), nil
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
