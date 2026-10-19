package polling

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/flux-iac/tofu-controller/internal/git/provider/providerfakes"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// The plan copy is built from what the pull request would apply, so a
// pull request that only changes the manifest is planned with its inputs.
func Test_reconcile_plansRenderedSpec(t *testing.T) {
	g := gomega.NewWithT(t)
	ns := newNamespace(t, g)
	ctx := t.Context()

	source := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: ns.Name},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://github.com/tf-controller/helloworld",
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1"},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, source))

	original := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{Name: "stack", Namespace: ns.Name},
		Spec: infrav1.TerraformSpec{
			Path:          "./tofu/modules/stack",
			SourceRef:     infrav1.CrossNamespaceSourceReference{Name: source.Name, Kind: "GitRepository"},
			BranchPlanner: &infrav1.BranchPlanner{EnablePathScope: true},
			Vars:          []infrav1.Variable{{Name: "environment", Value: &apiextensionsv1.JSON{Raw: []byte(`"live"`)}}},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, original))

	repo := provider.Repository{Org: "org", Name: "repo"}
	prs := []provider.PullRequest{
		{Repository: repo, Number: 1, HeadBranch: "manifest-change", HeadSha: "aaa"},
		{Repository: repo, Number: 2, HeadBranch: "pending", HeadSha: "bbb"},
		{Repository: repo, Number: 3, HeadBranch: "removes-stack", HeadSha: "ccc"},
		{Repository: repo, Number: 4, HeadBranch: "unrelated", HeadSha: "ddd"},
	}

	gitProvider := &providerfakes.FakeProvider{}
	gitProvider.ListPullRequestChangesCalls(func(_ context.Context, pr provider.PullRequest) ([]provider.Change, error) {
		if pr.Number == 4 {
			return []provider.Change{{Path: "docs/readme.md"}}, nil
		}
		return []provider.Change{{Path: "infrastructure/prod/stack.yaml"}}, nil
	})

	rendered := original.DeepCopy()
	rendered.Spec.Vars = []infrav1.Variable{{Name: "environment", Value: &apiextensionsv1.JSON{Raw: []byte(`"from-pull-request"`)}}}
	inputs := map[string]struct{}{"infrastructure/prod/stack.yaml": {}}
	pending := map[string]bool{"2": true}

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	server.render = func(_ context.Context, _ *infrav1.Terraform, _ *sourcev1.GitRepository, pr provider.PullRequest, prID string) (*prRender, error) {
		switch {
		case pending[prID]:
			return nil, errRenderPending
		case pr.Number == 3:
			return &prRender{inputs: inputs}, nil
		default:
			return &prRender{terraform: rendered.DeepCopy(), inputs: inputs}, nil
		}
	}

	expectToSucceed(t, g, server.reconcile(ctx, original, source, prs, gitProvider))

	copies := planCopies(t, g, ns.Name, original.Name)
	g.Expect(copies).To(gomega.HaveLen(1), "only the manifest change is planned: #2 waits, #3 removes the stack, #4 is out of scope")
	pr1 := copies["1"]
	g.Expect(pr1).NotTo(gomega.BeNil())
	g.Expect(pr1.Spec.Vars).To(gomega.HaveLen(1))
	g.Expect(string(pr1.Spec.Vars[0].Value.Raw)).To(gomega.Equal(`"from-pull-request"`))
	g.Expect(pr1.Spec.PlanOnly).To(gomega.BeTrue())

	// While a pull request's render is pending, its existing plan copy stays.
	pending["1"] = true
	expectToSucceed(t, g, server.reconcile(ctx, original, source, prs, gitProvider))
	g.Expect(planCopies(t, g, ns.Name, original.Name)).To(gomega.HaveKey("1"))
}

func planCopies(t *testing.T, g gomega.Gomega, namespace, primary string) map[string]*infrav1.Terraform {
	t.Helper()
	list := &infrav1.TerraformList{}
	expectToSucceed(t, g, k8sClient.List(t.Context(), list, client.InNamespace(namespace), client.MatchingLabels{
		bpconfig.LabelKey:                bpconfig.LabelValue,
		bpconfig.LabelPrimaryResourceKey: primary,
	}))
	out := map[string]*infrav1.Terraform{}
	for i := range list.Items {
		out[list.Items[i].Labels[bpconfig.LabelPRIDKey]] = &list.Items[i]
	}
	return out
}
