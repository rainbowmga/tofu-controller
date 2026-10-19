package polling

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/flux-iac/tofu-controller/internal/git/provider/providerfakes"
)

var renderFixture = map[string]string{
	"infrastructure/base/stack.yaml": `apiVersion: infra.contrib.fluxcd.io/v1alpha2
kind: Terraform
metadata:
  name: stack
  namespace: flux-system
spec:
  path: ./tofu/modules/stack
  interval: 1h
  sourceRef:
    kind: GitRepository
    name: release
  vars:
    - name: environment
      value: ${ENVIRONMENT}
    - name: region
      value: ${REGION}
`,
	"infrastructure/base/kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - stack.yaml
`,
	"infrastructure/prod/kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base
  - other.yaml
`,
	"infrastructure/prod/other.yaml": `apiVersion: infra.contrib.fluxcd.io/v1alpha2
kind: Terraform
metadata:
  name: other
  namespace: flux-system
  annotations:
    kustomize.toolkit.fluxcd.io/substitute: disabled
spec:
  path: ./tofu/modules/other
  interval: 1h
  sourceRef:
    kind: GitRepository
    name: release
  vars:
    - name: literal
      value: ${NOT_SUBSTITUTED}
`,
	"infrastructure/unrelated/kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
`,
}

func writeFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for path, content := range files {
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func fixtureKustomization() unstructured.Unstructured {
	ks := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata": map[string]any{
			"name":      "root-prod",
			"namespace": "flux-system",
		},
		"spec": map[string]any{
			"path": "./infrastructure/prod",
			"sourceRef": map[string]any{
				"kind": "GitRepository",
				"name": "release",
			},
			"patches": []any{
				map[string]any{
					"patch": "apiVersion: infra.contrib.fluxcd.io/v1alpha2\nkind: Terraform\nmetadata:\n  name: any\nspec:\n  storeReadablePlan: human\n",
					"target": map[string]any{
						"group": "infra.contrib.fluxcd.io",
						"kind":  "Terraform",
					},
				},
			},
			"postBuild": map[string]any{
				"substitute": map[string]any{
					"ENVIRONMENT": "prod",
				},
				"substituteFrom": []any{
					map[string]any{"kind": "ConfigMap", "name": "cluster-vars"},
				},
			},
		},
	}}
	return ks
}

func fixtureClient() *fake.ClientBuilder {
	return fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-vars", Namespace: "flux-system"},
		Data:       map[string]string{"REGION": "us-west-2"},
	})
}

func varsOf(tf *infrav1.Terraform) map[string]string {
	vars := map[string]string{}
	for _, v := range tf.Spec.Vars {
		vars[v.Name] = string(v.Value.Raw)
	}
	return vars
}

func Test_renderDir(t *testing.T) {
	g := gomega.NewWithT(t)
	root := writeFixture(t, renderFixture)

	entry, err := renderDir(context.Background(), fixtureClient().Build(), fixtureKustomization(), root)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	byName := map[string]*infrav1.Terraform{}
	for _, tf := range entry.terraforms {
		byName[tf.Name] = tf
	}
	g.Expect(byName).To(gomega.HaveLen(2))

	stack := byName["stack"]
	g.Expect(stack.Spec.StoreReadablePlan).To(gomega.Equal("human"), "Kustomization patches apply")
	g.Expect(varsOf(stack)).To(gomega.Equal(map[string]string{
		"environment": `"prod"`,
		"region":      `"us-west-2"`,
	}), "inline and substituteFrom variables apply")

	other := byName["other"]
	g.Expect(other.Spec.StoreReadablePlan).To(gomega.Equal("human"))
	g.Expect(varsOf(other)["literal"]).To(gomega.Equal(`"${NOT_SUBSTITUTED}"`), "substitution can be disabled per object")

	g.Expect(entry.inputs).To(gomega.HaveKey("infrastructure/base/stack.yaml"))
	g.Expect(entry.inputs).To(gomega.HaveKey("infrastructure/base/kustomization.yaml"))
	g.Expect(entry.inputs).To(gomega.HaveKey("infrastructure/prod/other.yaml"))
	g.Expect(entry.inputs).NotTo(gomega.HaveKey("infrastructure/unrelated/kustomization.yaml"))
}

func Test_renderDir_missingPath(t *testing.T) {
	g := gomega.NewWithT(t)
	root := writeFixture(t, renderFixture)
	ks := fixtureKustomization()
	g.Expect(unstructured.SetNestedField(ks.Object, "./infrastructure/missing", "spec", "path")).To(gomega.Succeed())

	_, err := renderDir(context.Background(), fixtureClient().Build(), ks, root)
	g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring("not found")))
}

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for path, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func Test_renderArtifact(t *testing.T) {
	g := gomega.NewWithT(t)
	archive := tarGz(t, renderFixture)
	sum := sha256.Sum256(archive)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archive)
	}))
	defer srv.Close()

	entry, err := renderArtifact(context.Background(), fixtureClient().Build(), fixtureKustomization(), srv.URL+"/a.tar.gz", "sha256:"+hex.EncodeToString(sum[:]))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(entry.terraforms).To(gomega.HaveLen(2))

	_, err = renderArtifact(context.Background(), fixtureClient().Build(), fixtureKustomization(), srv.URL+"/a.tar.gz", "sha256:"+hex.EncodeToString(make([]byte, 32)))
	g.Expect(err).To(gomega.HaveOccurred(), "a digest mismatch fails the fetch")
}

func Test_repoKey(t *testing.T) {
	g := gomega.NewWithT(t)
	https, err := repoKey("https://github.com/Org/Repo.git")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	ssh, err := repoKey("ssh://git@github.com/org/repo")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	other, err := repoKey("https://github.com/org/other")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(https).To(gomega.Equal(ssh))
	g.Expect(https).NotTo(gomega.Equal(other))
}

func Test_pullRequestInScope(t *testing.T) {
	tf := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{Name: "stack", Namespace: "flux-system"},
		Spec: infrav1.TerraformSpec{
			Path:          "./tofu/modules/stack",
			BranchPlanner: &infrav1.BranchPlanner{EnablePathScope: true},
		},
	}
	render := &prRender{
		terraform: tf.DeepCopy(),
		inputs: map[string]struct{}{
			"infrastructure/prod/stack.yaml": {},
		},
	}

	cases := []struct {
		name    string
		changes []provider.Change
		render  *prRender
		want    bool
	}{
		{"module change", []provider.Change{{Path: "tofu/modules/stack/main.tf"}}, nil, true},
		{"manifest change is planned with a render", []provider.Change{{Path: "infrastructure/prod/stack.yaml"}}, render, true},
		{"manifest change needs a render", []provider.Change{{Path: "infrastructure/prod/stack.yaml"}}, nil, false},
		{"renamed manifest", []provider.Change{{Path: "infrastructure/prod/renamed.yaml", PreviousPath: "infrastructure/prod/stack.yaml"}}, render, true},
		{"unrelated change", []provider.Change{{Path: "docs/readme.md"}}, render, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			gitProvider := &providerfakes.FakeProvider{}
			gitProvider.ListPullRequestChangesReturns(c.changes, nil)
			s := &Server{log: logr.Discard()}
			g.Expect(s.pullRequestInScope(context.Background(), tf, gitProvider, provider.PullRequest{Number: 1}, c.render)).To(gomega.Equal(c.want))
		})
	}
}
