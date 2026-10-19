package polling

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/kustomize"
	securefs "github.com/fluxcd/pkg/kustomize/filesys"
	"github.com/fluxcd/pkg/tar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/kustomize/kyaml/filesys"

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
// head of the pull request yet.
var errRenderPending = errors.New("render source is not at the pull request head yet")

// prRender is a Terraform object as a pull request would apply it, rendered
// from the pull request revision of the Flux Kustomization that applies it,
// and the repository paths that render read.
type prRender struct {
	terraform *infrav1.Terraform
	inputs    map[string]struct{}
}

// renderCache keeps renders per Kustomization and pull request head, so the
// stacks one Kustomization applies share a single download and build.
type renderCache struct {
	mu      sync.Mutex
	entries map[string]renderEntry
}

type renderEntry struct {
	terraforms []*infrav1.Terraform
	inputs     map[string]struct{}
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
	if normalizeURL(ksSource.Spec.URL) != normalizeURL(source.Spec.URL) {
		return nil, nil
	}

	renderSource, err := s.reconcileRenderSource(ctx, ksSource, pr.HeadBranch, prID)
	if err != nil {
		return nil, err
	}
	artifact := renderSource.GetArtifact()
	if artifact == nil || !strings.HasSuffix(artifact.Revision, pr.HeadSha) {
		if err := s.requestReconcile(ctx, renderSource, pr.HeadSha); err != nil {
			return nil, err
		}
		return nil, errRenderPending
	}

	key := fmt.Sprintf("%s/%s@%s", ksNamespace, ksName, artifact.Revision)
	entry, ok := s.renders.get(key)
	if !ok {
		archive, err := s.downloadArtifact(ctx, artifact.URL, artifact.Digest)
		if err != nil {
			return nil, err
		}
		entry, err = renderArchive(ctx, s.clusterClient, *ks, archive)
		if err != nil {
			return nil, fmt.Errorf("unable to render Kustomization %s/%s at %s: %w", ksNamespace, ksName, artifact.Revision, err)
		}
		s.renders.put(key, entry)
	}

	for _, tf := range entry.terraforms {
		if tf.Name == original.Name && (tf.Namespace == original.Namespace || tf.Namespace == "") {
			return &prRender{terraform: tf.DeepCopy(), inputs: entry.inputs}, nil
		}
	}

	// The pull request removes this stack, so there is nothing to plan.
	return &prRender{inputs: entry.inputs}, nil
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
// tracking the pull request branch, for rendering Kustomizations.
func (s *Server) reconcileRenderSource(ctx context.Context, ksSource *sourcev1.GitRepository, branch, prID string) (*sourcev1.GitRepository, error) {
	repo := repoKey(ksSource.Spec.URL)
	src := &sourcev1.GitRepository{
		ObjectMeta: renderSourceMeta(ksSource.Namespace, repo, prID),
	}

	_, err := controllerutil.CreateOrUpdate(ctx, s.clusterClient, src, func() error {
		spec := ksSource.Spec.DeepCopy()
		spec.Reference = &sourcev1.GitRepositoryRef{Branch: branch}
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
	list := &sourcev1.GitRepositoryList{}
	if err := s.clusterClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{
		bpconfig.LabelKey: bpconfig.LabelValue,
		renderSourceLabel: repoKey(url),
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

func (s *Server) downloadArtifact(ctx context.Context, url, digest string) ([]byte, error) {
	if hostname := os.Getenv("SOURCE_CONTROLLER_LOCALHOST"); hostname != "" {
		if i := strings.Index(url, "://"); i >= 0 {
			rest := url[i+3:]
			if j := strings.Index(rest, "/"); j >= 0 {
				url = url[:i+3] + hostname + rest[j:]
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to download artifact: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unable to download artifact from %s: %s", url, resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read artifact: %w", err)
	}

	if algo, want, ok := strings.Cut(digest, ":"); ok && algo == "sha256" {
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != want {
			return nil, fmt.Errorf("artifact digest mismatch: want %s, got sha256:%s", digest, got)
		}
	}
	return data, nil
}

// renderArchive builds the Kustomization from a source artifact the way
// kustomize-controller does, and returns the Terraform objects it produces
// and the repository paths the build read.
func renderArchive(ctx context.Context, kubeClient client.Client, ks unstructured.Unstructured, archive []byte) (renderEntry, error) {
	root, err := os.MkdirTemp("", "branch-planner-render-")
	if err != nil {
		return renderEntry{}, err
	}
	defer os.RemoveAll(root)

	if err := tar.Untar(bytes.NewReader(archive), root, tar.WithMaxUntarSize(tar.UnlimitedUntarSize)); err != nil {
		return renderEntry{}, fmt.Errorf("unable to extract artifact: %w", err)
	}
	return renderDir(ctx, kubeClient, ks, root)
}

func renderDir(ctx context.Context, kubeClient client.Client, ks unstructured.Unstructured, root string) (renderEntry, error) {
	// The secure filesystem resolves symlinks, so resolve root too, or the
	// recorded paths would not be relative to it.
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return renderEntry{}, err
	}

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

	disk, err := securefs.MakeFsOnDiskSecure(root)
	if err != nil {
		return renderEntry{}, err
	}
	fs := &recordingFS{FileSystem: disk, root: root, read: map[string]struct{}{}}
	resMap, err := kustomize.Build(fs, dirPath)
	if err != nil {
		return renderEntry{}, fmt.Errorf("kustomize build failed: %w", err)
	}

	_, hasPostBuild, _ := unstructured.NestedMap(ks.Object, "spec", "postBuild")

	entry := renderEntry{inputs: fs.read}
	for _, res := range resMap.Resources() {
		if res.GetKind() != infrav1.TerraformKind || !strings.HasPrefix(res.GetApiVersion(), infrav1.GroupVersion.Group+"/") {
			continue
		}
		if hasPostBuild {
			substituted, err := kustomize.SubstituteVariables(ctx, kubeClient, ks, res)
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

// recordingFS records every file a kustomize build reads, as slash separated
// paths relative to root, so a pull request can be matched against them.
type recordingFS struct {
	filesys.FileSystem
	root string
	mu   sync.Mutex
	read map[string]struct{}
}

func (f *recordingFS) record(path string) {
	rel, err := filepath.Rel(f.root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return
	}
	f.mu.Lock()
	f.read[filepath.ToSlash(rel)] = struct{}{}
	f.mu.Unlock()
}

func (f *recordingFS) ReadFile(path string) ([]byte, error) {
	f.record(path)
	return f.FileSystem.ReadFile(path)
}

func (f *recordingFS) Open(path string) (filesys.File, error) {
	f.record(path)
	return f.FileSystem.Open(path)
}

func normalizeURL(url string) string {
	return strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(url), "/"), ".git")
}

func repoKey(url string) string {
	sum := sha256.Sum256([]byte(normalizeURL(url)))
	return hex.EncodeToString(sum[:])[:16]
}

func renderSourceMeta(namespace, repo, prID string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: namespace,
		Name:      bpconfig.PullRequestObjectName("branch-planner-render-"+repo, prID),
	}
}
