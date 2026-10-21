# rainbowmga fork

Branch `rainbow/v0.16.5` is upstream `v0.16.5` plus the patches below. Only the `branch-planner` image is built from it (`ghcr.io/rainbowmga/branch-planner:<tag>`, on tags `v*-rainbow.*`); the controller and runners stay on upstream images.

- Branch planner posts a PR comment when `tofu plan` fails (`TFExecPlanFailed`), not only on init and post-planning webhook failures.
- Branch planner sources track only the pull request branch. A copied `tag`, `semver`, `name` or `commit` used to take precedence over the branch.
- `!replan` clears the error marker for the revision, so a replan that fails again reports its error instead of leaving the "Planning in progress..." placeholder.
- Plan, error and "Planning in progress" comments name the stack they are for, since one pull request can plan several stacks.

- Branch planner plans what the pull request would apply: it renders the stack's Flux Kustomization at the pull request head (patches, components, postBuild substitution) and plans that object, not the one in the cluster. A pull request is in scope when it touches the module or when the rendered object differs from the render at the revision the Kustomization applies, so a manifest change plans the stacks it changes and only those. Needs `get`/`list` on Kustomizations and `update`/`patch` on GitRepositories, a writable `/tmp`, and keeps one render GitRepository per open pull request.
- `!replan` is acted on once: a comment older than the plan copy, its last plan or a replan already requested is ignored, instead of posting "Planning in progress..." on every poll until the plan completes.
- A plan is commented on once: the informer records the plan time it commented on in an `infra.weave.works/commented-plan-at` annotation on the plan copy, so its own annotation writes and other updates of the object no longer post the same plan again. The record is on the object, so a controller restart keeps it.
- Branch planner plan copies carry `event.toolkit.fluxcd.io/branch` and `event.toolkit.fluxcd.io/pull-request` annotations, so Flux alerts on a failed pull request plan name the branch and pull request.
- Plan copies also carry `event.toolkit.fluxcd.io/pull-request-url`, so those alerts link to the pull request.
- `!ack` on a pull request suspends the failing plan copy at the current head: no retries and no alerts until new commits arrive, or until `!replan`. The planner confirms the acknowledgement with one comment.
- Pull requests are planned from GitHub's merge of the pull request onto its base (`refs/pull/N/merge`), not from the head branch, so a plan shows what merging would apply instead of reverting what the base gained since the branch point, with no rebase needed. While GitHub has not computed the merge the previous plan stays; a conflicting pull request is told so once and not planned. Providers without a merge ref plan the head branch.

The fixes are meant to go upstream; drop each patch once a release includes it.
