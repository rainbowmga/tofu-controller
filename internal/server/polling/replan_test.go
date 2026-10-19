package polling

import (
	"testing"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
