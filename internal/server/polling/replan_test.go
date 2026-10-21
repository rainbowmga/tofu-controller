package polling

import (
	"context"
	"testing"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/flux-iac/tofu-controller/internal/git/provider/providerfakes"
)

// A replan runs against the revision whose error was already reported, so it
// must clear the error marker or the informer never comments on the new run.
func Test_replanTerraform_clearsErrorRevision(t *testing.T) {
	g := gomega.NewWithT(t)
	ns := newNamespace(t, g)

	tf := &infrav1.Terraform{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "helloworld-pr-1",
			Namespace: ns.Name,
			Annotations: map[string]string{
				bpconfig.AnnotationErrorRevision: "feature@sha1:abc",
				"keep-me":                        "yes",
			},
		},
		Spec: infrav1.TerraformSpec{
			SourceRef: infrav1.CrossNamespaceSourceReference{
				Kind:      "GitRepository",
				Name:      "source",
				Namespace: ns.Name,
			},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(t.Context(), tf))

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	expectToSucceed(t, g, server.replanTerraform(t.Context(), tf, 42))

	got := &infrav1.Terraform{}
	expectToSucceed(t, g, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(tf), got))
	g.Expect(got.Annotations).NotTo(gomega.HaveKey(bpconfig.AnnotationErrorRevision))
	g.Expect(got.Annotations).To(gomega.HaveKeyWithValue(bpconfig.AnnotationCommentIDKey, "42"))
	g.Expect(got.Annotations).To(gomega.HaveKey(meta.ReconcileRequestAnnotation))
	g.Expect(got.Annotations).To(gomega.HaveKeyWithValue("keep-me", "yes"))
}

// A "!replan" comment is acted on once. One that predates the plan copy was
// never about it, and one already turned into a replan request must not post
// a placeholder on every poll until that plan completes.
func Test_reconcile_replanOnce(t *testing.T) {
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

	pr := provider.PullRequest{Repository: provider.Repository{Org: "org", Name: "repo"}, Number: 1, HeadBranch: "change"}

	type comment struct {
		at   time.Time
		body string
	}
	comments := []comment{{time.Now().Add(-time.Hour), "!replan"}}
	gitProvider := &providerfakes.FakeProvider{}
	gitProvider.GetLastCommentsCalls(func(_ context.Context, _ provider.PullRequest, since time.Time) ([]*provider.Comment, error) {
		var out []*provider.Comment
		for _, c := range comments {
			if c.at.After(since) {
				out = append(out, &provider.Comment{Body: c.body})
			}
		}
		return out, nil
	})
	gitProvider.AddCommentToPullRequestReturns(&provider.Comment{ID: 42}, nil)

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	server.render = func(context.Context, *infrav1.Terraform, *sourcev1.GitRepository, provider.PullRequest, string) (*prRender, error) {
		return nil, nil
	}

	expectToSucceed(t, g, server.reconcile(ctx, original, source, []provider.PullRequest{pr}, gitProvider))
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(0), "a comment older than the plan copy is ignored")

	comments = append(comments, comment{time.Now(), "!replan"})
	expectToSucceed(t, g, server.reconcile(ctx, original, source, []provider.PullRequest{pr}, gitProvider))
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(1), "a new comment posts one placeholder")

	copy := planCopies(t, g, ns.Name, original.Name)["1"]
	g.Expect(copy).NotTo(gomega.BeNil())
	g.Expect(copy.Annotations[bpconfig.AnnotationCommentIDKey]).To(gomega.Equal("42"))
	g.Expect(copy.Annotations[meta.ReconcileRequestAnnotation]).NotTo(gomega.BeEmpty())

	expectToSucceed(t, g, server.reconcile(ctx, original, source, []provider.PullRequest{pr}, gitProvider))
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(1), "the pending replan is not requested again")
}

// "!ack" suspends a plan copy at the pull request head until the head moves
// or a "!replan" asks for a plan, and is confirmed once.
func Test_reconcile_acknowledge(t *testing.T) {
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
		HeadBranch: "change",
		HeadSha:    "aaa",
		Link:       "https://github.com/org/repo/pull/1",
	}

	type comment struct {
		at   time.Time
		body string
	}
	var comments []comment
	gitProvider := &providerfakes.FakeProvider{}
	gitProvider.GetLastCommentsCalls(func(_ context.Context, _ provider.PullRequest, since time.Time) ([]*provider.Comment, error) {
		var out []*provider.Comment
		for i := len(comments) - 1; i >= 0; i-- {
			if comments[i].at.After(since) {
				out = append(out, &provider.Comment{Body: comments[i].body})
			}
		}
		return out, nil
	})
	gitProvider.AddCommentToPullRequestReturns(&provider.Comment{ID: 42}, nil)

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	server.render = func(context.Context, *infrav1.Terraform, *sourcev1.GitRepository, provider.PullRequest, string) (*prRender, error) {
		return nil, nil
	}
	poll := func() *infrav1.Terraform {
		t.Helper()
		expectToSucceed(t, g, server.reconcile(ctx, original, source, []provider.PullRequest{pr}, gitProvider))
		copy := planCopies(t, g, ns.Name, original.Name)["1"]
		g.Expect(copy).NotTo(gomega.BeNil())
		return copy
	}

	copy := poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeFalse())
	g.Expect(copy.Annotations[bpconfig.AnnotationEventPullRequestURL]).To(gomega.Equal(pr.Link), "alerts link to the pull request")

	comments = append(comments, comment{time.Now(), "!ack"})
	copy = poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeTrue(), "acknowledged at the head")
	g.Expect(copy.Annotations[bpconfig.AnnotationAcknowledgedRevision]).To(gomega.Equal("aaa"))
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(1), "acknowledgement is confirmed")
	_, _, body := gitProvider.AddCommentToPullRequestArgsForCall(0)
	g.Expect(string(body)).To(gomega.ContainSubstring("Acknowledged: `stack`"))

	copy = poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeTrue(), "the planner's own rewrite keeps the suspension")
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(1), "the same comment is not confirmed again")

	pr.HeadSha = "bbb"
	copy = poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeFalse(), "new commits lift the acknowledgement")
	g.Expect(copy.Annotations[bpconfig.AnnotationAcknowledgedRevision]).To(gomega.Equal("aaa"), "the stale record keeps the old comment from being acted on again")
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(1))

	comments = append(comments, comment{time.Now(), "!ack"})
	copy = poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeTrue())
	g.Expect(copy.Annotations[bpconfig.AnnotationAcknowledgedRevision]).To(gomega.Equal("bbb"))
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(2))

	comments = append(comments, comment{time.Now(), "!replan"})
	copy = poll()
	g.Expect(copy.Spec.Suspend).To(gomega.BeFalse(), "a newer !replan plans the same head")
	g.Expect(copy.Annotations).NotTo(gomega.HaveKey(bpconfig.AnnotationAcknowledgedRevision))
	g.Expect(copy.Annotations[meta.ReconcileRequestAnnotation]).NotTo(gomega.BeEmpty())
	g.Expect(gitProvider.AddCommentToPullRequestCallCount()).To(gomega.Equal(3), "the replan placeholder")
}
