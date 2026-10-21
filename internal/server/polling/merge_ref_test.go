package polling

import (
	"context"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/flux-iac/tofu-controller/internal/git/provider/providerfakes"
)

// A pull request is planned from its merge onto the base, so the plan is what
// merging applies. While the provider has not merged it yet the existing plan
// stays, and a conflicting pull request is told so once and not planned.
func Test_reconcile_plansMergeResult(t *testing.T) {
	g := gomega.NewWithT(t)
	ns := newNamespace(t, g)
	ctx := t.Context()

	source := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: ns.Name},
		Spec: sourcev1.GitRepositorySpec{
			URL:       "https://github.com/tf-controller/helloworld",
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, source))

	original := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{Name: "stack", Namespace: ns.Name},
		Spec: infrav1.TerraformSpec{
			Path:      "./tofu/modules/stack",
			SourceRef: infrav1.CrossNamespaceSourceReference{Name: source.Name, Kind: "GitRepository"},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(ctx, original))

	pr := provider.PullRequest{
		Repository: provider.Repository{Org: "org", Name: "repo"},
		Number:     1,
		BaseBranch: "main",
		HeadBranch: "change",
		HeadSha:    "aaa",
		MergeRef:   "refs/pull/1/merge",
		MergeSha:   "mmm",
	}

	fresh := pr
	var posted []*provider.Comment
	gitProvider := &providerfakes.FakeProvider{}
	gitProvider.GetPullRequestCalls(func(context.Context, provider.Repository, int) (provider.PullRequest, error) {
		return fresh, nil
	})
	gitProvider.AddCommentToPullRequestCalls(func(_ context.Context, _ provider.PullRequest, body []byte) (*provider.Comment, error) {
		posted = append(posted, &provider.Comment{ID: len(posted) + 1, Body: string(body)})
		return posted[len(posted)-1], nil
	})
	gitProvider.GetLastCommentsCalls(func(context.Context, provider.PullRequest, time.Time) ([]*provider.Comment, error) {
		return posted, nil
	})

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	server.render = func(context.Context, *infrav1.Terraform, *sourcev1.GitRepository, provider.PullRequest, string) (*prRender, error) {
		return nil, nil
	}
	poll := func() map[string]*infrav1.Terraform {
		t.Helper()
		expectToSucceed(t, g, server.reconcile(ctx, original, source, []provider.PullRequest{pr}, gitProvider))
		return planCopies(t, g, ns.Name, original.Name)
	}
	sourceOf := func(copy *infrav1.Terraform) sourcev1.GitRepositoryRef {
		t.Helper()
		src := &sourcev1.GitRepository{}
		expectToSucceed(t, g, k8sClient.Get(ctx, client.ObjectKey{Name: copy.Spec.SourceRef.Name, Namespace: copy.Spec.SourceRef.Namespace}, src))
		return *src.Spec.Reference
	}

	copy := poll()["1"]
	g.Expect(copy).NotTo(gomega.BeNil())
	g.Expect(sourceOf(copy)).To(gomega.Equal(sourcev1.GitRepositoryRef{Name: "refs/pull/1/merge"}), "the merge onto the base is planned")

	// The list has no merge commit yet and fetching the pull request does not
	// produce one either: the plan from the last merge stays.
	pr.MergeSha = ""
	fresh.MergeSha = ""
	copy = poll()["1"]
	g.Expect(copy).NotTo(gomega.BeNil(), "the existing plan copy is kept while the merge is computed")
	g.Expect(sourceOf(copy)).To(gomega.Equal(sourcev1.GitRepositoryRef{Name: "refs/pull/1/merge"}))
	g.Expect(gitProvider.GetPullRequestCallCount()).To(gomega.Equal(1), "the pull request is fetched to have the merge computed")

	// Fetching the pull request reports a conflict: no plan, one comment.
	fresh.Conflicting = true
	g.Expect(poll()).NotTo(gomega.HaveKey("1"), "a conflicting pull request is not planned")
	g.Expect(posted).To(gomega.HaveLen(1))
	g.Expect(posted[0].Body).To(gomega.ContainSubstring("conflicts with `main` at aaa"))
	poll()
	g.Expect(posted).To(gomega.HaveLen(1), "the conflict is reported once per head")

	// Resolved: the provider merges it again and the plan follows.
	pr.MergeSha = "nnn"
	fresh = pr
	copy = poll()["1"]
	g.Expect(copy).NotTo(gomega.BeNil())
	g.Expect(sourceOf(copy)).To(gomega.Equal(sourcev1.GitRepositoryRef{Name: "refs/pull/1/merge"}))

	// A provider without a merge ref plans the head branch.
	pr.MergeRef, pr.MergeSha = "", ""
	copy = poll()["1"]
	g.Expect(copy).NotTo(gomega.BeNil())
	g.Expect(sourceOf(copy)).To(gomega.Equal(sourcev1.GitRepositoryRef{Branch: "change"}))
}
