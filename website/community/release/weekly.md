---
title: Weekly source releases
sidebar_position: 4
---

# Weekly source releases

Each Friday, Actions prepares a version PR from the Friday 00:00 UTC main cutoff.
Merging that PR validates, builds, signs and uploads the candidate to ATR. The
release manager starts a 72-hour Trusted Vote; ATR counts ballots and can
resolve and publish a passing vote automatically. Monday availability depends on
when the vote actually starts and whether it passes.

## Configure

Use the existing `GPG_SECRET_KEY` secret and `SOURCE_SIGNING_FINGERPRINT` variable.
GitHub operations use the built-in `GITHUB_TOKEN`; no personal GitHub token, ATR
token or ASF ID is stored in CI. Allow Actions to create pull requests.

In ATR, select Trusted Vote and configure repository `opendal`, leaving the
optional branch restriction empty. Register `.github/workflows/weekly_release.yml`
for compose and finish. Keep `.github/workflows/release-compose.yml` registered
for compose if direct manual dispatch is also used. The reusable compose workflow
retains the caller's OIDC identity.

The scheduled actor must be an ASF-linked committer with project permissions.
GitHub associates the schedule with the user who last changes its cron expression;
an RM should maintain it. Manual runs and candidate PR merges must likewise be
initiated by an ASF-linked project release manager.

## Prepare and review

The weekly workflow records main push metadata. Preparation selects the latest
recorded push at or before Friday 00:00 UTC, rather than using commit author dates
or a delayed runner's current main. With no earlier snapshot, wait for the next
week. Retain push-run history until the cutoff has been consumed.

Version calculation uses the newest Git-tagged source release present in the
public distribution area. An announced ATR release with an RC tag can supply the
baseline before its final tag or main synchronization exists. Drafts and failed
votes never consume a stable version. The complete package matrix receives at
least a patch increment; higher versions already reviewed in `package.rs` are
preserved. Review breaking changes in the version PR and update that inventory
before preparation. The existing Rust compatibility validator remains in use;
there is no separate release-declaration format.

Each attempt creates `release-candidates/<version>-rc.<attempt>` and a version PR
from its `-bump` branch. The RC suffix combines the Actions run ID and run attempt;
it is an opaque identifier, not a small sequential counter. Review and merge the
version PR into the candidate branch. Its merged SHA becomes the immutable RC tag.
Keep candidate branches and RC tags until publication and synchronization finish.

The merge event invokes existing check and dev workflows, then compose, as jobs in
the same run. Successful compose uploads the signed artifacts under the stable
version directory in ATR and produces `vote-request.json` as a run artifact.
No workflow waits across the human review or vote, and no `release-state` branch
or custom retry journal is created.

## Start the vote

The RM reviews the candidate, revision and vote text in the handoff, waits for ATR
checks to finish, and starts the vote in ATR or through their locally authenticated
official ATR client. Refresh acknowledged concerns if checks completed after the
handoff was generated. OpenDAL's own checks determine quality; ATR blockers still
prevent opening a vote. CI never submits `/vote/start` or casts ballots.

Select Trusted Vote, at least 72 hours, notification on completion, automatic
resolution and automatic publication. The prepared request includes these options.
For a rehearsal, automatic publication must remain disabled. Public release
queries do not establish these per-vote options; the RM verifies them at opening.

The hourly follow-up adds the formal ATR link to the version PR. PMC members
independently verify signatures and reproduce the archives, then vote in ATR.
GitHub comments are discussion only. At expiry, a passing vote can resolve and
publish automatically; otherwise the RM waits or cancels in ATR.

After publication, follow-up compares every distributed archive, signature and
checksum with the signed Actions artifact. It then uses ASF's OIDC action to
announce, and waits for ATR's `release` phase before creating the final tag and a
PR back to main. ATR handles email delivery asynchronously. Retain the successful
run's signed artifact until this comparison completes. If it expires, the RM must
verify the original candidate and finish through ATR; do not reconstruct evidence
by signing a different build under the old RC identity.

Source release is the scope of this workflow. Language package publication remains
with its existing workflows. Tags created with `GITHUB_TOKEN` do not trigger those
publishers automatically.

## Failure and rehearsal

For preparation, validation or build failures, run `prepare` again in the Actions
UI. It recalculates from the same successful baseline and current week's cutoff,
using a new RC and branches. Abandoned branches, PRs and ATR drafts may remain;
the RM can clean them up. Do not rerun compose for an already uploaded candidate.
If only handoff generation failed, obtain the revision and vote options from ATR
and the run's candidate information; uploading again is unnecessary.

An open ATR vote blocks replacement until the RM cancels it. An approved release
in preview blocks new preparation until publication finishes. Hourly `follow-up`
reads ATR again and continues publication of the existing files. Before retrying
an uncertain announcement, inspect ATR's phase; do not send a separate email.
A published candidate is never replaced to work around a follow-up failure.

Use `rehearsal` to prepare from current main with a rehearsal branch. Review and
merge its version PR, then start the real rehearsal vote with automatic publication
disabled. Rehearsals do not receive final tags or main synchronization PRs. Remove
the rehearsal from active vote/preview state in ATR before starting another real
candidate. There are no special recovery commands or enablement variables.

## Validation boundary

Local tests cover fresh attempts, entrypoint routing, manual handoff, immutable
candidate selection, version baselines and publication verification. Deployment
must verify repository PR permissions, the initiating actor, reusable workflow
OIDC matching and announcement against live GitHub/ATR. Installing this workflow
on main activates both schedules; this PR alone does not constitute a deployment.
