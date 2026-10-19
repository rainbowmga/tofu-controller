# rainbowmga fork

Branch `rainbow/v0.16.5` is upstream `v0.16.5` plus the patches below. Only the `branch-planner` image is built from it (`ghcr.io/rainbowmga/branch-planner:<tag>`, on tags `v*-rainbow.*`); the controller and runners stay on upstream images.

- Branch planner posts a PR comment when `tofu plan` fails (`TFExecPlanFailed`), not only on init and post-planning webhook failures.
- Branch planner sources track only the pull request branch. A copied `tag`, `semver`, `name` or `commit` used to take precedence over the branch.
- `!replan` clears the error marker for the revision, so a replan that fails again reports its error instead of leaving the "Planning in progress..." placeholder.

- Branch planner plans what the pull request would apply: it renders the stack's Flux Kustomization at the pull request head (patches, components, postBuild substitution) and plans that object, not the one in the cluster. A pull request is in scope when it touches the module or any file the render read, so manifest-only changes get a plan. Needs `get`/`list` on Kustomizations and `update`/`patch` on GitRepositories, and keeps one render GitRepository per open pull request.

The fixes are meant to go upstream; drop each patch once a release includes it.
