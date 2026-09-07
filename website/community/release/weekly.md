---
title: Weekly source releases
sidebar_position: 4
---

# Weekly source releases

OpenDAL prepares a source release from the Friday 00:00 UTC cutoff. A version PR
is reviewed and merged into a release branch, after which GitHub Actions validates,
builds, signs and uploads the candidate. ATR hosts the formal Trusted Vote and
publishes the approved source files. GitHub Discussions provides a reminder and
space for discussion, not a second ballot box.

Monday availability is a target. The 72-hour voting period begins with ATR's vote
announcement, after preparation and validation complete. An unresolved vote blocks
the next candidate until the release manager cancels it or completes the release.

This controller publishes source archives only. Maven, crates.io, PyPI, npm,
NuGet, website deployment and other distribution channels retain their existing
workflows. Its tags are created using the repository `GITHUB_TOKEN`, so they do
not trigger the existing tag-based publishers.

## Configure once

The source compose workflow uses the existing `GPG_SECRET_KEY` secret and
`SOURCE_SIGNING_FINGERPRINT` variable. The weekly controller uses GitHub's
short-lived repository `GITHUB_TOKEN`. It requires no additional personal GitHub
or ATR token, and no ASF ID variable.

Enable Actions to create pull requests in the repository's workflow permissions.
The workflow requests contents, pull requests, discussions and Actions write
permissions. Explicit validation dispatches work with `GITHUB_TOKEN`; generated
pushes and PRs do not automatically trigger further workflows.

Configure ATR Trusted Publishing for repository `opendal`, with the optional
branch restriction empty. Register `.github/workflows/weekly_release.yml` under
both **compose** and **finish**. Keep `.github/workflows/release-compose.yml` under
compose for direct manual use. The weekly workflow calls compose as a reusable
workflow, preserving the initiating actor and caller workflow identity for OIDC.
Announcement uses ASF's `release-on-atr` action with OIDC.

The scheduled workflow actor must be linked to an ASF committer account with the
required project permissions. GitHub associates scheduled runs with the user who
last changed the cron schedule; a manual run uses its initiating user. An RM
should maintain the schedule and initiate recovery runs. This identity comes
from GitHub OIDC, not an ASF ID supplied in repository configuration.

The RM and voters keep their personal ATR authentication in their local official
ATR clients. The controller cannot start or cancel a vote or cast a ballot.

Review `.release/config.json` before deployment. `baseline_tag` identifies the
last official source release for initialization, not a failed RC. Initialization
checks its public distribution directory and records its package versions. After
that, persisted state determines the successful baseline. There is no enablement
variable, GitHub environment approval, or recurring initialization operation.
Installing the workflow on the default branch activates its schedule.

`release-state/state.json` is created automatically on the first reconciliation.
It contains candidate identities, version plans, artifact digests, workflow and
ATR task IDs, and publication progress. It never contains credentials. Writes use
GitHub's content SHA comparison and the workflow runs serially. Restrict writes
to this branch to release maintainers; do not rewrite it to recover a failed run.


## Candidate preparation

Main push workflow metadata records the head and server timestamp. A delayed
scheduler chooses the latest recorded push at or before Friday 00:00 UTC; it does
not use commit author dates or the later main head. If no push snapshot predates
the cutoff, the controller waits for a later cycle instead of guessing. Keep the
weekly workflow's run history until its cutoff has been consumed.

A week with no unreleased source changes is skipped. Version synchronization PRs
created by the controller do not themselves cause another release. The controller
starts from the last successful package versions, increments the complete source
package matrix by at least a patch, and preserves larger versions already declared
in `dev/src/release/package.rs`. Public dependency compatibility is also checked by
`update-version --baseline`.

A normal PR can supply compatibility information in an added, uniquely named
`.release/changes/*.json` file:

```json
{
  "summary": "Explain the user-visible change",
  "packages": {"core": "breaking"}
}
```

The supported levels are `patch`, `feature`, and `breaking`. Breaking changes to
pre-1.0 packages raise the minor version; post-1.0 packages raise the major version.
Breaking core changes also raise integration compatibility versions. Package names
are the paths in the source package inventory. A changed inventory requires a
reviewed baseline migration. Classification is reviewed by humans; the controller
does not infer API compatibility from commit messages. Existing declaration files
should not be edited or removed; add corrections in a new file.

The controller creates `release-candidates/<cycle>` at the cutoff and a `-bump`
branch containing generated versions, dependency inventories and changelog. The
version PR targets the release branch. Review and merge that PR to continue.
The resulting commit is frozen for CI, the RC tag, signing and voting. If the
release branch changes afterward, cancel and prepare a fresh candidate rather
than rebuilding different sources under the same identity.

`ci_odev.yml` and `ci_check.yml` are explicitly dispatched and must pass before
compose. Compose runs as part of the weekly run, without another workflow dispatch.
Source reproducibility and signature validation also run in compose.
ATR concerns and suggestions do not introduce another project quality gate:
the prepared vote request includes the acknowledged concerns. ATR's server still
requires its background checks to complete and refuses candidates with blockers.

## Trusted Vote and publication

The candidate is uploaded under `<stable-version>/` inside ATR. This preserves
OpenDAL's versioned SVN distribution layout when ATR's automatic publisher uses
an empty download suffix. The ATR candidate key includes the RC suffix; source
archive names and the published directory use their stable package versions.

After upload, the controller records the immutable signed Actions artifact's
SHA-512 digests and checks the ATR inventory. It enters `awaiting-vote` and writes
`vote-request.json` to the `weekly-vote-request` artifact and persisted state.
The request fixes the candidate revision, vote text and acknowledged concerns.
Repeated ticks do not start a vote or send vote mail.

### RM: start the vote

Download the handoff from the weekly run. Review its candidate SHA, ATR revision
and vote text, then start the vote in ATR's web interface, or authorize your local
agent to submit the prepared request using your official ATR client's identity.
The request goes to `/api/vote/start`; it is never submitted by CI. Retain the
returned task ID if using the API so an uncertain response can be reconciled
without sending another vote email.

Use these settings:

- Trusted Vote with a minimum duration of 72 hours.
- Notify when finished and automatically resolve when finished.
- Automatically publish when resolved for a real release.
- Disable automatic publication for a rehearsal.

The supplied JSON sets these values, including `automatic_resolve_when_finished`
and `automatic_publish_when_resolved`. When using the web interface, set the same
options explicitly. The controller's public release query cannot confirm those
per-vote automation settings; the RM owns that check when opening the vote.

The controller observes the manually started Trusted Vote and requires it to
match the recorded revision. It creates the GitHub reminder, downloads the frozen
candidate and compares every archive, signature and checksum with the signed
Actions artifact. A mismatch stops follow-up and asks the RM to cancel in ATR.
Voters independently rebuild on trusted hardware, inspect the source, and submit
their own authorized ballots through ATR's website or API. GitHub comments and
email replies are discussion, not recorded Trusted Vote ballots.

ATR resolves a passing vote and publishes its exact files to SVN. If the vote has
not passed at expiry, the RM can cancel it in ATR; the controller does not invent a
result or start a competing candidate. An API/client failure is reported in the
workflow summary and persisted without discarding the candidate.

The controller checks all published bytes on `downloads.apache.org` before
advancing its baseline. It then uses the OIDC announcement action and, after ATR reports the release
phase, creates a
PR from the released branch back to main. Normal PR review handles conflicts.
The next version calculation uses the successful baseline even while that PR is
pending. Publication already in progress must be recovered using the same
candidate; it cannot be replaced by next week's head.

## Rehearse and recover

Start a rehearsal from the Actions UI or:

```shell
gh workflow run weekly_release.yml --repo apache/opendal -f operation=rehearsal
```

A rehearsal uses the current main head and an unused RC number, then follows the
same version PR, validation, signing and upload flow, followed by the RM starting
a real Trusted Vote. Its vote
and GitHub reminder explicitly identify it as a rehearsal. Automatic SVN
publication is false; it never announces an official release, creates a final tag,
synchronizes main or advances the successful baseline. Candidate data may remain
in ATR. Rehearsal success ends at a passing vote; the RM cancels an unsuccessful
vote in ATR.

Routine ticks resume work without rebuilding completed artifacts. Rerun failed
validation workflows through GitHub Actions. For failed compose, rerun **all jobs**
of the original weekly run so its signing artifact and run attempt stay aligned.
The controller adopts that run's successful retry. Once voting begins, never rerun compose for that candidate.

Recovery operations take the candidate ID shown in state and the workflow summary:

- `cancel`: abandon preparation, or acknowledge a vote already cancelled in ATR.
  It refuses candidates in publication or later phases.
- `retry-dispatch`: after confirming no workflow run was created, clear that
  validation workflow's uncertain dispatch marker and allow another dispatch.
- `retry-announce`: after confirming ATR did not send the announcement, allow a
  new announcement request. Never use it merely because a response timed out.

All operations have a manual Actions entry point; none requires an environment
approval. A cancelled or failed candidate does not consume a stable version. Next
week's fresh cutoff is recalculated from the last successful release, with a new
RC number. An active candidate always takes precedence over starting a new one.

## Validation boundary

Local lifecycle tests cover preparation, manual vote handoff, fixed revisions,
reusable compose recovery and publication follow-up. A live deployment must still
verify the scheduled actor, repository token permissions, ATR caller-workflow
matching and OIDC announcement. ATR's `release` phase confirms that it accepted
the announcement; email delivery is handled asynchronously by ATR.
