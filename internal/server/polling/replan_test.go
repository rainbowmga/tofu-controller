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
