package polling

import (
	"context"
	"testing"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/flux-iac/tofu-controller/internal/git/provider/providerfakes"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// A plan copy and its source go to the plan namespace and run as the plan
// service account, and copies left next to the stack are deleted.
func Test_reconcile_plansInPlanNamespaceAsPlanIdentity(t *testing.T) {
	g := gomega.NewWithT(t)
	ns := newNamespace(t, g)
	planNS := newNamespace(t, g)
	ctx := t.Context()

	source := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "infra", Namespace: ns.Name},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://github.com/tf-controller/helloworld",
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
			SecretRef: &meta.LocalObjectReference{Name: "git-deploy-key"},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, source))

	original := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{Name: "stack", Namespace: ns.Name},
		Spec: infrav1.TerraformSpec{
			Path:               "./tofu/modules/stack",
			SourceRef:          infrav1.CrossNamespaceSourceReference{Name: source.Name, Kind: "GitRepository"},
			ServiceAccountName: "tf-runner",
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, original))

	misplaced := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bpconfig.PullRequestObjectName(original.Name, "7"),
			Namespace: ns.Name,
			Labels: map[string]string{
				bpconfig.LabelKey:                bpconfig.LabelValue,
				bpconfig.LabelPrimaryResourceKey: original.Name,
				bpconfig.LabelPRIDKey:            "7",
			},
		},
		Spec: original.Spec,
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, misplaced))

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	server.plan = bpconfig.Plan{Namespace: planNS.Name, ServiceAccountName: "tf-planner"}
	server.render = func(_ context.Context, _ *infrav1.Terraform, _ *sourcev1.GitRepository, _ provider.PullRequest, _ string) (*prRender, error) {
		return nil, nil
	}

	repo := provider.Repository{Org: "org", Name: "repo"}
	prs := []provider.PullRequest{{Repository: repo, Number: 7, HeadBranch: "change", HeadSha: "aaa"}}
	expectToSucceed(t, g, server.reconcile(ctx, original, source, prs, &providerfakes.FakeProvider{}))

	g.Expect(planCopies(t, g, ns.Name, original.Name)).To(gomega.BeEmpty(), "no copy stays next to the stack")

	copies := planCopies(t, g, planNS.Name, original.Name)
	g.Expect(copies).To(gomega.HaveKey("7"))
	planned := copies["7"]
	g.Expect(planned.Spec.ServiceAccountName).To(gomega.Equal("tf-planner"))
	g.Expect(planned.Spec.SourceRef.Namespace).To(gomega.Equal(planNS.Name))

	planSource := &sourcev1.GitRepository{}
	expectToSucceed(t, g, k8sClient.Get(ctx, client.ObjectKey{Namespace: planNS.Name, Name: planned.Spec.SourceRef.Name}, planSource))
	g.Expect(planSource.Spec.Reference.Branch).To(gomega.Equal("change"))
	g.Expect(planSource.Spec.SecretRef.Name).To(gomega.Equal("git-deploy-key"), "the source reads its credential from the plan namespace")
}
