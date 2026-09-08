# Release Vote

Repository paths and shell commands are relative to the repository root.

## Pre-Vote Readiness Checklist

Run this checklist immediately before creating the vote discussion:

- RC tag exists and points to the intended commit.
- A newer `main` SHA does not invalidate an existing RC by itself; the vote is on the RC tag and uploaded artifacts.
- Required RC workflows are `completed/success`.
- The selected ATR revision or `dist/dev/opendal/${release_version}/` contains the signed source artifacts being proposed.
- Artifact filenames match the package-specific versions from `dev/src/release/package.rs`; signatures and hashes have been independently verified.
- `KEYS` URL is reachable: `https://downloads.apache.org/opendal/KEYS`.
- Maven staging URL returns success and is not an open/hidden staging repo.
- TestPyPI project URL is reachable: `https://test.pypi.org/project/opendal/`.
- Staged website URL is reachable: `https://opendal-v${release_version with dots replaced by hyphens}.staged.apache.org/`.
- `scripts/verify.py` is reachable from the RC tag.

Do not start a vote if any checklist item fails unless the release manager
explicitly narrows the gate or waives a non-source-package readiness issue.

If TestPyPI publish failed only because a file already exists from the same RC
attempt, distinguish that from missing artifacts. Report the exact duplicate
filename and proceed only with an explicit release-manager waiver.

A weekly candidate-ready Discussion is a preparation notice, not a vote. For
ATR candidates, inspect ATR checks and pin the verified candidate revision; use
the live ATR vote path and its artifact links. Do not substitute an unpopulated
SVN directory in the vote text. The template below applies to SVN-staged votes.

## Start Vote Discussion

Create the discussion in the `General` category of `apache/opendal`.

Use the repository runbook template, with:

- Title: `[VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1`
- Source packages: `https://dist.apache.org/repos/dist/dev/opendal/${release_version}/`
- Git tag: `https://github.com/apache/opendal/releases/tag/v${release_version}`
- Maven staging repo: `https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/`
- Website: `https://opendal-v${release_version with dots replaced by hyphens}.staged.apache.org/`
- Verify command:

```bash
svn co https://dist.apache.org/repos/dist/dev/opendal/${release_version}/ opendal-dev
cd opendal-dev
curl -sSL https://github.com/apache/opendal/raw/v${release_version}/scripts/verify.py -o verify.py
python verify.py
```

Use `gh api graphql` by default. Creating the discussion requires the repository id and the `General` category id, not just the category name:

```bash
gh api graphql -F query='
query {
  repository(owner: "apache", name: "opendal") {
    id
    discussionCategories(first: 20) {
      nodes { id name slug }
    }
  }
}'
```

Then call `createDiscussion` with the resolved repository id, category id, title, and body. Avoid hand-editing multiline bodies through escaped `\n`; use a body file or stdin.

```bash
gh api graphql \
  -F repositoryId="${repository_id}" \
  -F categoryId="${category_id}" \
  -F title="${vote_title}" \
  -F body=@/tmp/opendal-vote.md \
  -F query='
mutation($repositoryId: ID!, $categoryId: ID!, $title: String!, $body: String!) {
  createDiscussion(input: {repositoryId: $repositoryId, categoryId: $categoryId, title: $title, body: $body}) {
    discussion { number url }
  }
}'
```

## Vote Result

The vote must stay open for at least 72 hours unless the release manager explicitly declares an emergency case.

Before claiming the result:

- Count only valid binding votes from OpenDAL PMC members as binding.
- Require at least 3 `+1` binding votes.
- Require more `+1` binding votes than `-1` binding votes.
- Use voters' real names, public profile names, or Apache IDs in the result.
- Check that the vote discussion is not closed and that a result discussion has not already been posted.

Create the result discussion with:

- Title: `[RESULT][VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1`
- Body containing binding votes, non-binding votes, `+0`, `-1`, and the vote thread URL.

Do not declare an ASF release official just because the vote looks promising. Wait for the formal result.
