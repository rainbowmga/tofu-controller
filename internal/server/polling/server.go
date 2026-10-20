package polling

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bpconfig "github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

const DefaultPollingInterval = time.Second * 30

type Server struct {
	log                   logr.Logger
	clusterClient         client.Client
	configMapRef          client.ObjectKey
	pollingInterval       time.Duration
	branchPollingInterval time.Duration
	allowedNamespaces     []string
	noCrossNamespaceRefs  bool
	gitProviderParserFn   provider.URLParserFn

	// render returns a Terraform object as a pull request would apply it.
	// Tests replace it; it defaults to renderForPullRequest.
	render  func(ctx context.Context, original *infrav1.Terraform, source *sourcev1.GitRepository, pr provider.PullRequest, prID string) (*prRender, error)
	renders renderCache
}

func New(options ...Option) (*Server, error) {
	server := &Server{
		log:                 logr.Discard(),
		gitProviderParserFn: provider.FromURL,
	}

	server.render = server.renderForPullRequest

	for _, opt := range options {
		if err := opt(server); err != nil {
			return nil, err
		}
	}

	return server, nil
}

func (s *Server) Start(ctx context.Context) error {
	tick := time.Tick(s.pollingInterval)
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-tick:
			// Read the config in each iteration. The idea behind this decision is to
			// allow the user to change the list of resources without the need of
			// restart of the pod.
			// It can be a bit smarter like using a time.Ticker and refresh config
			// periodically.
			config, err := s.readConfig(ctx)
			if err != nil {
				return err
			}

			secret, err := s.getSecret(ctx, client.ObjectKey{
				Namespace: config.SecretNamespace,
				Name:      config.SecretName,
			})
			if err != nil {
				s.log.Error(err, "failed to get secret")
			}

			for _, resource := range config.Resources {
				if resource.Namespace == "" {
					resource.Namespace = bpconfig.RuntimeNamespace()
				}

				if !s.isNamespaceAllowed(resource.Namespace) {
					s.log.Info("skip resource because namespace is not allowed", "namespace", resource.Namespace)

					continue
				}

				if resource.Name != "" {
					if err := s.poll(ctx, resource, secret); err != nil {
						s.log.Error(err, "failed to check pull request")
					}

					continue
				}

				s.log.Info("checking all Terraform objects in namespace", "namespace", resource.Namespace)

				resources, err := s.listTerraformObjects(ctx, resource.Namespace, nil)
				if err != nil {
					s.log.Error(err, "failed to list Terraform objects in namespace", "namespace", resource.Namespace)

					continue
				}
				s.log.Info("found Terraform objects", "count", len(resources))

				for _, tf := range resources {
					s.log.Info("checking Terraform object", "namespace", tf.Namespace, "name", tf.Name)

					// Skip if the object is the Terraform planner object
					if tf.Labels[bpconfig.LabelKey] == bpconfig.LabelValue {
						continue
					}

					resource := types.NamespacedName{
						Namespace: tf.Namespace,
						Name:      tf.Name,
					}

					if err := s.poll(ctx, resource, secret); err != nil {
						s.log.Error(err, "failed to check pull request")
					}
				}
			}
		}
	}
}

func (s *Server) poll(ctx context.Context, resource types.NamespacedName, secret *corev1.Secret) error {
	s.log.Info("start polling", "namespace", resource.Namespace, "name", resource.Name)

	if secret == nil {
		err := fmt.Errorf("secret is not defined")
		s.log.Error(err, "failed to get secret")
		return err
	}

	s.log.Info("fetching terraform object", "namespace", resource.Namespace, "name", resource.Name)
	tf, err := s.getTerraformObject(ctx, resource)
	if err != nil {
		s.log.Error(err, "failed to get terraform object", "namespace", resource.Namespace, "name", resource.Name)
		return fmt.Errorf("failed to get terraform object %s/%s: %w", resource.Namespace, resource.Name, err)
	}

	s.log.Info("fetching source object", "namespace", resource.Namespace, "name", resource.Name)
	source, err := s.getSource(ctx, tf)
	if err != nil {
		s.log.Error(err, "failed to get source object")
		return fmt.Errorf("failed to get source object: %w", err)
	}

	s.log.Info("initializing git provider", "url", source.Spec.URL)
	secretOpts, err := provider.OptsFromSecret(secret.Data)
	if err != nil {
		s.log.Error(err, "failed to parse provider secret")
		return fmt.Errorf("failed to parse provider secret: %w", err)
	}

	opts := append([]provider.ProviderOption{provider.WithLogger(s.log)}, secretOpts...)
	gitProvider, repo, err := s.gitProviderParserFn(source.Spec.URL, opts...)

	if err != nil {
		s.log.Error(err, "failed to get git provider")
		return fmt.Errorf("failed to get git provider: %w", err)
	}

	s.log.Info("listing pull requests")
	prs, err := gitProvider.ListPullRequests(ctx, repo)
	if err != nil {
		s.log.Error(err, "failed to list pull requests")
		return fmt.Errorf("failed to list pull requests: %w", err)
	}

	s.log.Info("reconciling pull requests")
	return s.reconcile(ctx, tf, source, prs, gitProvider)
}

// pullRequestInScope reports whether a pull request changes what tf would
// plan: its module, or with a render, any file the render of its
// Kustomization read, such as the manifest that defines tf.
func (s *Server) pullRequestInScope(ctx context.Context, tf *infrav1.Terraform, gitProvider provider.Provider, pr provider.PullRequest, render *prRender) bool {
	if tf.Spec.BranchPlanner == nil || !tf.Spec.BranchPlanner.EnablePathScope {
		return true
	}

	prefixes := []string{strings.TrimLeft(tf.Spec.Path, "./")}
	if render != nil && render.terraform != nil {
		prefixes = append(prefixes, strings.TrimLeft(render.terraform.Spec.Path, "./"))
	}
	if slices.Contains(prefixes, "") {
		return true
	}

	changes, err := gitProvider.ListPullRequestChanges(ctx, pr)
	if err != nil {
		s.log.Error(err, "can't list pull request changes", "PR ID", pr.Number, "name", tf.Name, "namespace", tf.Namespace)
	}

	for _, change := range changes {
		for _, path := range []string{change.Path, change.PreviousPath} {
			if path == "" {
				continue
			}
			for _, prefix := range prefixes {
				if strings.HasPrefix(path, prefix) {
					s.log.Info("has terraform changed", "path", path)
					return true
				}
			}
			if render != nil {
				if _, ok := render.inputs[path]; ok {
					s.log.Info("has manifest changed", "path", path)
					return true
				}
			}
		}
	}

	return false
}

func (s *Server) reconcile(ctx context.Context, original *infrav1.Terraform, source *sourcev1.GitRepository, prs []provider.PullRequest, gitProvider provider.Provider) error {
	log := s.log.WithValues("terraform", original.Name, "namespace", original.Namespace, "source", source.Name)

	log.Info("starting reconciliation ...")

	// Create a map of the pull requests that get a plan, with the PR number as
	// the key and the PR itself as the value.
	prMap := map[string]provider.PullRequest{}
	open := map[string]bool{}
	for _, pr := range prs {
		prId := fmt.Sprintf("%d", pr.Number)
		if pr.Closed {
			continue
		}
		open[prId] = true

		// Plan the object the pull request would apply, rendered from its
		// Kustomization at the pull request head, rather than the object in
		// the cluster.
		render, err := s.render(ctx, original, source, pr, prId)
		if errors.Is(err, errRenderPending) {
			// Keep any existing plan object until the source catches up.
			log.Info("waiting for the pull request source to reach the head revision", "PR ID", prId)
			prMap[prId] = pr
			continue
		}
		if err != nil {
			log.Error(err, "unable to render the pull request, planning the in-cluster object", "PR ID", prId)
			render = nil
		}
		if render != nil && render.terraform == nil {
			log.Info("the pull request removes this object, nothing to plan", "PR ID", prId)
			continue
		}

		if !s.pullRequestInScope(ctx, original, gitProvider, pr, render) {
			continue
		}

		prMap[prId] = pr
		log.Info("mapping PR", "PR ID", prId)

		spec := original.Spec
		if render != nil {
			spec = plannedSpec(original.Spec, render.terraform.Spec)
		}

		// Reconcile the Terraform objects related to each PR.
		// If an error occurs, log it and continue with the next PR.
		if err := s.reconcileTerraform(ctx, original, spec, source, pr.HeadBranch, prId, s.branchPollingInterval); err != nil {
			log.Error(err, "failed to reconcile Terraform object for PR", "PR ID", prId)
		} else {
			log.Info("successfully reconciled Terraform object for PR", "PR ID", prId)
		}
	}

	if err := s.deleteStaleRenderSources(ctx, source.Namespace, source.Spec.URL, open); err != nil {
		log.Error(err, "failed to delete render sources of closed pull requests")
	}

	// List the Terraform planner objects in the namespace of the original object
	log.Info("listing Terraform objects...")
	tfPlannerObjects, err := s.listTerraformObjects(ctx, original.Namespace, map[string]string{
		bpconfig.LabelKey:                bpconfig.LabelValue,
		bpconfig.LabelPrimaryResourceKey: original.Name,
	})

	// If an error occurs, wrap it with context information and return it.
	if err != nil {
		return fmt.Errorf("failed to list Terraform objects: %w", err)
	}

	log.Info("iterating over Terraform planner objects...")
	// For each Terraform object created by the branch planner,
	// check if there's a corresponding open PR. If not, delete the Terraform object.
	for _, tfPlannerObject := range tfPlannerObjects {
		prId := tfPlannerObject.Labels[bpconfig.LabelPRIDKey]
		pr, exist := prMap[prId]
		// If the PR does not exist or has been closed, delete the related Terraform object.
		// If an error occurs, log it.
		if !exist || pr.Closed {
			log.Info("the PR either does not exist or has been closed, deleting corresponding Terraform object...", "PR ID", prId)
			if err = s.deleteTerraformAndSource(ctx, tfPlannerObject); err != nil {
				log.Error(err, "failed to delete Terraform object", "name", tfPlannerObject.Name, "namespace", tfPlannerObject.Namespace, "PR ID", prId)
			} else {
				log.Info("successfully deleted Terraform object", "name", tfPlannerObject.Name, "namespace", tfPlannerObject.Namespace, "PR ID", prId)
			}

			// If the PR does not exist, continue with the next Terraform object.
			continue
		}

		lastPlanAt := time.Time{}
		if tfPlannerObject.Status.LastPlanAt != nil {
			lastPlanAt = tfPlannerObject.Status.LastPlanAt.Time
		}

		// check last comment, if it's "!replan" then trigger the replan action for the tfPlannerObject
		log.Info("checking last comment...")
		comments, err := gitProvider.GetLastComments(ctx, pr, lastPlanAt)
		if err != nil {
			log.Error(err, "failed to get last comment", "PR ID", prId)
		}

		// it was sorted by created time desc
		for _, comment := range comments {
			if comment != nil && strings.Contains(comment.Body, "!replan") {
				log.Info("last comment contains '!replan', triggering replan action...")

				placeholderComment, err := gitProvider.AddCommentToPullRequest(ctx, pr, []byte(fmt.Sprintf("Planning `%s` in progress...", original.Name)))
				if err != nil {
					log.Error(err, "failed to add comment to pull request", "PR ID", prId)
				} else {
					log.Info("successfully added comment to pull request", "PR ID", prId)
				}

				if err = s.replanTerraform(ctx, tfPlannerObject, placeholderComment.ID); err != nil {
					log.Error(err, "failed to trigger replan")
				} else {
					log.Info("successfully triggered replan", "PR ID", prId)
				}

				// found one comment with "!replan", no need to check the rest
				break
			}
		}
	}

	// If everything went well, return nil to indicate no errors occurred.
	log.Info("reconciliation process completed. Next run after: " + time.Now().Add(s.pollingInterval).Format(time.RFC3339))
	return nil
}

func (s *Server) replanTerraform(ctx context.Context, object *infrav1.Terraform, commentId int) error {
	terraform := &infrav1.Terraform{}
	// TODO use better namespaced name
	if err := s.clusterClient.Get(ctx, types.NamespacedName{Name: object.Name, Namespace: object.Namespace}, terraform); err != nil {
		return fmt.Errorf("failed to get terraform resource: %s", err)
	}
	patch := client.MergeFrom(terraform.DeepCopy())

	// clear the pending plan
	apimeta.SetStatusCondition(&terraform.Status.Conditions, metav1.Condition{
		Type:    meta.ReadyCondition,
		Status:  metav1.ConditionFalse,
		Reason:  "ReplanRequested",
		Message: "Replan requested",
	})

	terraform.Status.Plan.Pending = ""
	terraform.Status.LastPlannedRevision = ""
	terraform.Status.LastAttemptedRevision = ""
	statusOpts := &client.SubResourcePatchOptions{
		PatchOptions: client.PatchOptions{
			FieldManager: "tf-controller",
		},
	}
	if err := s.clusterClient.Status().Patch(ctx, terraform, patch, statusOpts); err != nil {
		return fmt.Errorf("failed to patch status of the terraform resource: %w", err)
	}

	// trigger a new reconcile
	if ann := terraform.GetAnnotations(); ann == nil {
		terraform.SetAnnotations(map[string]string{
			meta.ReconcileRequestAnnotation: time.Now().Format(time.RFC3339Nano),
			bpconfig.AnnotationCommentIDKey: strconv.Itoa(commentId),
		})
	} else {
		ann[meta.ReconcileRequestAnnotation] = time.Now().Format(time.RFC3339Nano)
		ann[bpconfig.AnnotationCommentIDKey] = strconv.Itoa(commentId)
		// The replan runs against the same revision, so an error already
		// reported for it would otherwise suppress the comment for this run.
		delete(ann, bpconfig.AnnotationErrorRevision)
		terraform.SetAnnotations(ann)
	}

	return s.clusterClient.Patch(ctx, terraform, patch)
}

func (s *Server) isNamespaceAllowed(name string) bool {
	if len(s.allowedNamespaces) == 0 {
		return true
	}

	return slices.Contains(s.allowedNamespaces, name)
}
