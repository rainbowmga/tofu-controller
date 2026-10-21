package provider

type PullRequest struct {
	Repository Repository
	Number     int
	BaseBranch string
	HeadBranch string
	BaseSha    string
	HeadSha    string
	Link       string
	Closed     bool

	// MergeRef names the provider's merge of the pull request onto its base,
	// such as refs/pull/N/merge, and MergeSha is its commit when the provider
	// has computed one; both are empty for providers without such a ref.
	MergeRef    string
	MergeSha    string
	Conflicting bool
}
