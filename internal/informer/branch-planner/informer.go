package branchplanner

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/flux-iac/tofu-controller/api/plan"
	infrav1 "github.com/flux-iac/tofu-controller/api/v1alpha2"
	"github.com/flux-iac/tofu-controller/internal/config"
	"github.com/flux-iac/tofu-controller/internal/git/provider"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	//go:embed plan-comment.tpl
	planCommentTemplate string

	//go:embed error-comment.tpl
	errorCommentTemplate string

	parsedPlanTemplate  = template.Must(template.New("plan-comment").Parse(planCommentTemplate))
	parsedErrorTemplate = template.Must(template.New("error-comment").Parse(errorCommentTemplate))
)

type Informer struct {
	sharedInformer cache.SharedIndexInformer
	handlers       cache.ResourceEventHandlerFuncs
	log            logr.Logger
	client         client.Client
	gitProvider    provider.Provider
	providerOpts   []provider.ProviderOption
	providerOnce   sync.Once
	providerErr    error

	mux    *sync.RWMutex
	synced bool
}

type Option func(s *Informer) error

func NewInformer(options ...Option) (*Informer, error) {
	informer := &Informer{}

	for _, opt := range options {
		if err := opt(informer); err != nil {
			return nil, err
		}
	}

	informer.mux = &sync.RWMutex{}
	informer.synced = false

	return informer, nil
}

func (i *Informer) HasSynced() bool {
	i.mux.RLock()
	defer i.mux.RUnlock()

	return i.synced
}

func (i *Informer) Start(ctx context.Context) error {
	if i.handlers.AddFunc == nil {
		i.handlers.AddFunc = i.addHandler
	}

	if i.handlers.UpdateFunc == nil {
		i.handlers.UpdateFunc = i.updateHandler
	}

	if i.handlers.DeleteFunc == nil {
		i.handlers.DeleteFunc = i.deleteHandler
	}

	i.sharedInformer.AddEventHandler(i.handlers)
	go i.sharedInformer.Run(ctx.Done())

	isSynced := cache.WaitForCacheSync(ctx.Done(), i.sharedInformer.HasSynced)
	i.mux.Lock()
	i.synced = isSynced
	i.mux.Unlock()

	if !i.synced {
		return fmt.Errorf("coudn't sync shared informer")
	}

	<-ctx.Done()

	return nil
}

func (i *Informer) SetAddHandler(fn func(any)) {
	i.handlers.AddFunc = fn
}

func (i *Informer) SetUpdateHandler(fn func(any, any)) {
	i.handlers.UpdateFunc = fn
}

func (i *Informer) SetDeleteHandler(fn func(any)) {
	i.handlers.DeleteFunc = fn
}

func (i *Informer) addHandler(obj any) {}

// updateHandler is called when a Terraform object is updated.
// It checks if the plan has been updated and if so, it creates a new PR comment to show the plan diff.
func (i *Informer) updateHandler(oldObj, newObj any) {
	if !i.synced {
		return
	}
	i.mux.RLock()
	defer i.mux.RUnlock()

	old := &infrav1.Terraform{}
	oldU, ok := oldObj.(*unstructured.Unstructured)
	if !ok {
		i.log.Info("previous object is not a unstructured.Unstructured object", "object", oldObj)

		return
	}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldU.Object, old)
	if err != nil {
		i.log.Error(err, "failed to convert previous object to Terraform object", "object", oldObj)
		return
	}

	new := &infrav1.Terraform{}
	newU, ok := newObj.(*unstructured.Unstructured)
	if !ok {
		i.log.Info("new object is not a unstructured.Unstructured object", "object", newObj)

		return
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newU.Object, new); err != nil {
		i.log.Error(err, "failed to convert current object to Terraform object", "object", newObj)
		return
	}

	ctx := context.Background()

	// The informer cache does not update when the status field is updated, so we need to get the object from api server.
	if err := i.client.Get(ctx, client.ObjectKeyFromObject(new), new); err != nil {
		i.log.Info("failed getting object", "namespace", new.Namespace, "name", new.Name, "error", err)
		return
	}

	for _, condition := range new.Status.Conditions {
		if condition.Reason == infrav1.TFExecInitFailedReason || condition.Reason == infrav1.TFExecPlanFailedReason || condition.Reason == infrav1.PostPlanningWebhookFailedReason {
			if ann := new.GetAnnotations(); ann != nil && ann[config.AnnotationErrorRevision] == new.Status.LastAttemptedRevision {
				break
			}

			i.log.Info("new error detected", "condition", condition, "name", new.Name)

			if err := i.addErrorAnnotation(ctx, new); err != nil {
				i.log.Error(err, "unable to add error annotation", "name", new.Name)

				// Do not create a comment if we couldn't add annotation. It's better to
				// not send the error and let the user debug (and be a bit angry), than
				// sending out 20 comments under a pull request with the same error
				// message.
				return
			}

			content, err := formatErrorOutput(stackName(new), condition.Message)
			if err != nil {
				i.log.Error(err, "failed to format error output")
				return
			}

			if _, replaced := i.addCommentToPullRequest(ctx, new, content); replaced {
				if err := i.patchAnnotations(ctx, new, nil, config.AnnotationCommentIDKey); err != nil {
					i.log.Error(err, "failed removing comment id from object", "namespace", new.Namespace, "name", new.Name)
				}
			}

			return
		}
	}

	if new.Labels[config.LabelKey] != config.LabelValue {
		i.log.Info("Terraform object is not managed by the branch-based planner")

		return
	}

	if !i.isNewPlan(old, new) {
		i.log.Info("Plan not updated", "namespace", new.Namespace, "name", new.Name, "pr-id", new.Labels[config.LabelPRIDKey])

		return
	}

	// The informer's previous object can lag behind the API server, so a
	// plan already commented on would otherwise be commented on again by the
	// next unrelated update of the object, such as the one this handler makes.
	stamp := new.Status.LastPlanAt.UTC().Format(time.RFC3339)
	if new.Annotations[config.AnnotationCommentedPlanAt] == stamp {
		i.log.Info("Plan already commented", "namespace", new.Namespace, "name", new.Name, "pr-id", new.Labels[config.LabelPRIDKey])

		return
	}

	plan, err := i.getPlan(ctx, new)
	if err != nil {
		i.log.Error(err, "get plan output")

		return
	}

	if len(plan) == 0 {
		i.log.Info("Empty plan output")

		return
	}

	i.log.Info("Updated plan", "pr-id", new.Labels[config.LabelPRIDKey])

	content, err := formatPlanOutput(stackName(new), plan, i.behindBase(ctx, new))
	if err != nil {
		i.log.Error(err, "failed to format plan output")
		return
	}

	if posted, _ := i.addCommentToPullRequest(ctx, new, content); posted {
		set := map[string]string{config.AnnotationCommentedPlanAt: stamp}
		if err := i.patchAnnotations(ctx, new, set, config.AnnotationCommentIDKey); err != nil {
			i.log.Error(err, "failed marking the plan as commented", "namespace", new.Namespace, "name", new.Name)
		}
	}
}

func (i *Informer) deleteHandler(obj any) {}

// addCommentToPullRequest posts content as a new comment, or as the new body
// of the placeholder whose id the object carries. It reports whether a
// comment was posted, and whether it replaced the placeholder.
func (i *Informer) addCommentToPullRequest(ctx context.Context, tf *infrav1.Terraform, content []byte) (posted, replaced bool) {
	repo, err := i.getRepo(ctx, tf)
	if err != nil {
		i.log.Error(err, "failed getting repository")
		return false, false
	}

	prId, err := strconv.Atoi(tf.Labels[config.LabelPRIDKey])
	if err != nil {
		i.log.Error(err, "failed converting PR id to integer", "pr-id", tf.Labels[config.LabelPRIDKey], "namespace", tf.Namespace, "name", tf.Name)
	}

	commentID, err := strconv.Atoi(tf.Annotations[config.AnnotationCommentIDKey])
	if err != nil {
		i.log.Error(err, "failed converting comment id to integer", "comment-id", tf.Annotations[config.AnnotationCommentIDKey], "namespace", tf.Namespace, "name", tf.Name)
		commentID = 0
	}

	pr := provider.PullRequest{
		Repository: repo,
		Number:     prId,
	}

	// If commentID is 0, it means that the comment has not been created yet.
	if commentID == 0 {
		if _, err := i.gitProvider.AddCommentToPullRequest(ctx, pr, content); err != nil {
			i.log.Error(err, "failed adding comment to pull request", "pr-id", tf.Labels[config.LabelPRIDKey], "namespace", tf.Namespace, "name", tf.Name)
			return false, false
		}
		return true, false
	}

	if err := i.gitProvider.UpdateCommentOfPullRequest(ctx, pr, commentID, content); err != nil {
		i.log.Error(err, "failed updating comment in pull request", "pr-id", tf.Labels[config.LabelPRIDKey], "comment-id", commentID, "namespace", tf.Namespace, "name", tf.Name)

		return false, false
	}

	return true, true
}

// patchAnnotations sets and removes annotations in one patch, so the single
// update event it causes already carries the whole outcome.
func (i *Informer) patchAnnotations(ctx context.Context, tf *infrav1.Terraform, set map[string]string, remove ...string) error {
	patch := client.MergeFrom(tf.DeepCopy())

	annotations := tf.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	for key, value := range set {
		annotations[key] = value
	}
	for _, key := range remove {
		delete(annotations, key)
	}
	tf.SetAnnotations(annotations)

	return i.client.Patch(ctx, tf, patch)
}

func (i *Informer) addErrorAnnotation(ctx context.Context, tf *infrav1.Terraform) error {
	patch := client.MergeFrom(tf.DeepCopy())

	if annotations := tf.GetAnnotations(); annotations == nil {
		tf.SetAnnotations(map[string]string{
			config.AnnotationErrorRevision: tf.Status.LastAttemptedRevision,
		})
	} else {
		annotations[config.AnnotationErrorRevision] = tf.Status.LastAttemptedRevision
		tf.SetAnnotations(annotations)
	}

	return i.client.Patch(ctx, tf, patch)
}

func (i *Informer) getPlan(ctx context.Context, obj *infrav1.Terraform) (string, error) {
	configMaps := &v1.ConfigMapList{}

	if err := i.client.List(ctx, configMaps, client.InNamespace(obj.GetNamespace()), client.MatchingLabels{
		plan.TFPlanNameLabel:      plan.SafeLabelValue(obj.GetName()),
		plan.TFPlanWorkspaceLabel: plan.SafeLabelValue(obj.WorkspaceName()),
	}); err != nil {
		return "", fmt.Errorf("unable to list plan configmaps: %s", err)
	}

	if len(configMaps.Items) == 0 {
		return "Please set `spec.storeReadablePlan: human` to view the plan", nil
	}

	tfPlan, err := plan.NewFromConfigMaps(obj.GetName(), obj.GetNamespace(), string(obj.GetUID()), configMaps.Items)
	if err != nil {
		return "", fmt.Errorf("unable to reconstruct plan from configmaps: %s", err)
	}

	return tfPlan.ToString(), nil
}

func (i *Informer) isNewPlan(old, new *infrav1.Terraform) bool {
	if new.Status.LastPlanAt == nil {
		return false
	}

	if old.Status.LastPlanAt == nil && new.Status.LastPlanAt != nil {
		return true
	}

	if new.Status.LastPlanAt.After(old.Status.LastPlanAt.Time) {
		return true
	}

	return false
}

func (i *Informer) getRepo(ctx context.Context, tf *infrav1.Terraform) (provider.Repository, error) {
	if tf.Spec.SourceRef.Kind != sourcev1.GitRepositoryKind {
		return provider.Repository{}, fmt.Errorf("branch based planner does not support source kind: %s", tf.Spec.SourceRef.Kind)
	}

	ref := client.ObjectKey{
		Namespace: tf.Spec.SourceRef.Namespace,
		Name:      tf.Spec.SourceRef.Name,
	}
	obj := &sourcev1.GitRepository{}
	if err := i.client.Get(ctx, ref, obj); err != nil {
		return provider.Repository{}, fmt.Errorf("unable to get Source: %w", err)
	}

	// Resolve the provider exactly once using sync.Once to avoid race
	// conditions when multiple update events arrive concurrently. Skip
	// resolution if a provider was already injected (e.g. via WithGitProvider
	// in tests).
	i.providerOnce.Do(func() {
		if i.gitProvider != nil {
			return
		}
		i.gitProvider, _, i.providerErr = provider.FromURL(obj.Spec.URL, i.providerOpts...)
	})
	if i.providerErr != nil {
		return provider.Repository{}, fmt.Errorf("failed resolving git provider from URL: %w", i.providerErr)
	}

	return provider.RepoFromURL(obj.Spec.URL)
}

// stackName is the stack a plan copy plans for, as named in the comments, so
// a pull request that plans several stacks reads as several plans.
func stackName(tf *infrav1.Terraform) string {
	if name := tf.Labels[config.LabelPrimaryResourceKey]; name != "" {
		return name
	}
	return tf.Name
}

// behindBase is how far the pull request's branch is behind its base. The
// plan is of the head branch, so a branch behind its base also plans
// reverting what the base gained; the comment says so.
type behindBase struct {
	Base   string
	Behind int
}

func (i *Informer) behindBase(ctx context.Context, tf *infrav1.Terraform) behindBase {
	repo, err := i.getRepo(ctx, tf)
	if err != nil {
		i.log.Error(err, "failed getting repository", "name", tf.Name)
		return behindBase{}
	}
	number, err := strconv.Atoi(tf.Labels[config.LabelPRIDKey])
	if err != nil {
		return behindBase{}
	}
	pr, err := i.gitProvider.GetPullRequest(ctx, repo, number)
	if err != nil {
		i.log.Error(err, "failed getting the pull request", "pr-id", number, "name", tf.Name)
		return behindBase{}
	}
	behind, err := i.gitProvider.CommitsBehindBase(ctx, pr)
	if err != nil {
		i.log.Error(err, "failed comparing the pull request with its base", "pr-id", number, "name", tf.Name)
		return behindBase{}
	}
	return behindBase{Base: pr.BaseBranch, Behind: behind}
}

func formatPlanOutput(stack, planOutput string, behind behindBase) ([]byte, error) {
	data := struct {
		Stack, PlanOutput string
		behindBase
	}{Stack: stack, PlanOutput: planOutput, behindBase: behind}

	var buf bytes.Buffer
	if err := parsedPlanTemplate.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("failed to format plan output: %w", err)
	}

	return buf.Bytes(), nil
}

func formatErrorOutput(stack, message string) ([]byte, error) {
	data := struct{ Stack, ErrorMessage string }{Stack: stack, ErrorMessage: message}

	var buf bytes.Buffer
	if err := parsedErrorTemplate.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("failed to format error output: %w", err)
	}

	return buf.Bytes(), nil
}
