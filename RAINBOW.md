# rainbowmga fork

Branch `rainbow/v0.16.5` is upstream `v0.16.5` plus the patches below. Only the `branch-planner` image is built from it (`ghcr.io/rainbowmga/branch-planner:<tag>`, on tags `v*-rainbow.*`); the controller and runners stay on upstream images.

- Branch planner posts a PR comment when `tofu plan` fails (`TFExecPlanFailed`), not only on init and post-planning webhook failures.
- Branch planner sources track only the pull request branch. A copied `tag`, `semver`, `name` or `commit` used to take precedence over the branch.

Both are meant to go upstream; drop each patch once a release includes it.
