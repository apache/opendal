---
title: Weekly source releases
sidebar_position: 4
---

# Weekly source releases

The weekly workflow prepares a release branch each Friday from the Friday
00:00 UTC main cutoff and directly calls the existing `release-compose.yml` to
build, sign and upload the candidate to ATR. Manual dispatch runs the same flow
from the selected branch commit at the time of dispatch.
Preparation creates no version PR and requires no manual merge.

The release manager then follows the [release procedure](release.md) to verify
the candidate, start the vote and finish publication.

## Preparation

Scheduled runs select the latest recorded main push at or before the cutoff.
Push-run timestamps record arrival on main; commit dates do not. Keep that run
history until preparation finishes. The first week requires a recorded push before
the cutoff. Manual dispatch starts a fresh attempt without requiring earlier push
history. It uses the dispatch commit even if the branch advances while the run
is queued.

`update-version --patch` prepares at least a patch increment for each package from
the latest reachable final release tag, preserving higher inventory versions and
versions of new packages. Final tags must identify successfully published releases.
It reuses the existing compatibility validation and package update logic. Breaking
changes may require editing the inventory before running preparation again.

Each attempt commits the version, dependency and changelog updates on a fresh
`release-candidates/weekly-<run>-<attempt>` branch. It pushes that branch and its RC
tag together, then passes the resulting SHA to compose in the same workflow run.
The RC suffix identifies the Actions run and attempt. Compatibility or preparation
failures stop the run before compose; correct the cause and start another run.

Preparing a new candidate does not cancel an existing vote or replace approved
artifacts. The RM resolves any existing vote or pending publication before starting
another vote. Abandoned branches and ATR drafts can be cleaned up separately.

## Configuration

Reuse the existing `GPG_SECRET_KEY` secret and `SOURCE_SIGNING_FINGERPRINT` variable.
Register the weekly workflow for ATR compose OIDC, since the reusable workflow
retains its caller's identity. The actor initiating a manual run or owning the
schedule must have the required ASF-linked project permissions. An RM should
maintain the cron expression, which determines the scheduled actor.

After pushing the RC tag, the workflow explicitly dispatches the existing tag
builds, binding checks and documentation workflow against that tag. GitHub does
not trigger downstream push workflows for tags created with `GITHUB_TOKEN`.
These dispatches retain the RC behavior: Java stages to Nexus, Python uses
TestPyPI, NodeJS performs a publish dry run, Ruby and .NET retain artifacts, and
Rust does not publish. Documentation retains RC staging without deploying to
nightlies. A retry skips workflows already dispatched for the candidate commit;
rerun failed downstream jobs from their own runs.

After ATR upload and dispatch succeed, the workflow posts a candidate preparation
notice in GitHub Discussions with the ATR, tag, source artifact and build links.
It reuses an existing notice for the same RC when retried. This notice does not
assert that downstream builds or ATR checks passed. The RM verifies those results
and the candidate before starting the formal vote.

The workflow does not start votes, announce final releases, create final tags or
synchronize main. These remain steps in the existing release procedure. Keep the
candidate branch and signed artifacts until that procedure is complete.

Installing this workflow on main activates the schedule. The initiating actor and
reusable workflow's ATR OIDC identity still require a live rehearsal; local checks
and this PR do not establish a successful remote upload.
