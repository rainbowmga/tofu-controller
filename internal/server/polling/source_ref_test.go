package polling

import (
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The branch source must track only the pull request branch: Flux resolves
// commit, name, semver and tag ahead of branch, so any of them left over from
// the original source would pin the plan to that ref.
func Test_reconcileSource_tracksOnlyBranch(t *testing.T) {
	g := gomega.NewWithT(t)
	ns := newNamespace(t, g)

	original := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "release-source",
			Namespace: ns.Name,
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:      "https://github.com/flux-iac/tofu-controller",
			Interval: metav1.Duration{Duration: time.Minute},
			Reference: &sourcev1.GitRepositoryRef{
				Branch: "main",
				Tag:    "v1.2.3",
				SemVer: ">=1.0.0",
				Name:   "refs/tags/v1.2.3",
				Commit: "0123456789abcdef0123456789abcdef01234567",
			},
		},
	}
	expectToSucceed(t, g, k8sClient.Create(t.Context(), original))

	server, err := New(WithClusterClient(k8sClient))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	source, err := server.reconcileSource(t.Context(), "helloworld", original, sourcev1.GitRepositoryRef{Branch: "feature-branch"}, "42", time.Minute)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	expectToEqual(t, g, *source.Spec.Reference, sourcev1.GitRepositoryRef{Branch: "feature-branch"})
	g.Expect(original.Spec.Reference.Tag).To(gomega.Equal("v1.2.3"), "the original source must not be modified")

	source, err = server.reconcileSource(t.Context(), "helloworld", original, sourcev1.GitRepositoryRef{Name: "refs/pull/42/merge"}, "42", time.Minute)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	expectToEqual(t, g, *source.Spec.Reference, sourcev1.GitRepositoryRef{Name: "refs/pull/42/merge"})
}

func Test_planRef(t *testing.T) {
	g := gomega.NewWithT(t)
	head := provider.PullRequest{HeadBranch: "feature", HeadSha: "aaa"}
	g.Expect(planRef(head)).To(gomega.Equal(sourcev1.GitRepositoryRef{Branch: "feature"}))
	g.Expect(planSha(head)).To(gomega.Equal("aaa"))

	merged := provider.PullRequest{HeadBranch: "feature", HeadSha: "aaa", MergeRef: "refs/pull/1/merge", MergeSha: "mmm"}
	g.Expect(planRef(merged)).To(gomega.Equal(sourcev1.GitRepositoryRef{Name: "refs/pull/1/merge"}), "the merge onto the base is planned when the provider has one")
	g.Expect(planSha(merged)).To(gomega.Equal("mmm"))

	uncomputed := provider.PullRequest{HeadBranch: "feature", HeadSha: "aaa", MergeRef: "refs/pull/1/merge"}
	g.Expect(planRef(uncomputed)).To(gomega.Equal(sourcev1.GitRepositoryRef{Branch: "feature"}), "callers wait for the merge instead; the head is the fallback")
}
