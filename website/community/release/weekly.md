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
lightweight tag together, then passes the resulting SHA to compose in the same workflow run.
Compose signs the source archives; the lightweight tag itself is unsigned.
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

## Builds and preparation notice

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

## Recovering an existing candidate

Inspect the weekly run's revision and jobs before assuming automatic dispatch is
available. Workflow changes must be present in the revision used by the run;
merging a fix does not backfill older candidates. List runs for the existing tag
without filtering to `push`, since automatic dispatch produces
`workflow_dispatch` events:

```bash
gh run list --repo apache/opendal --branch "v${release_candidate_version}" \
  --limit 100 --json name,event,headSha,status,conclusion,databaseId,url
```

Check each run's head SHA against the candidate commit. Rerun failed downstream
jobs from their existing runs. If a workflow has no run, dispatch it against the
same RC tag. For example, to recover a missing Python release build:

```bash
gh workflow run release_python.yml --repo apache/opendal \
  --ref "v${release_candidate_version}"
```

This RC path publishes to TestPyPI. Read the selected workflow at the RC tag
before dispatching: it must support `workflow_dispatch` there, not only on main.
Use the workflow list and inputs in the weekly `builds` job as the maintained
reference for other missing builds. In particular, NodeJS uses
`nodejs-publish=false` and `nodejs-publish-dry-run=false` (the RC tag enables its
dry run), .NET uses `release_type=none`, and Docs uses the RC tag as
`release_version` with `deploy-nightlies=false`. An old tag without a required
input or dispatch trigger needs a separate recovery decision; do not move it.

A successful dispatch only confirms acceptance. Find the resulting run and follow
it to completion. A candidate notice does not replace checking all required
builds, registry staging and ATR checks.

For a compose permission failure, correct ATR's allowed caller or actor
configuration and rerun failed jobs on the original run when its completed
outputs remain usable. Reuse existing signed artifacts. Rerunning preparation or
starting a new weekly run creates a new candidate rather than recovering the old
one.

## After the vote passes

The RM follows [Official Release](release.md#official-release): create the final
signed tag at the approved RC commit and push it with the RM's credentials. That
tag push triggers the existing final-release workflows. Weekly automation does
not evaluate the vote or create this tag. Nexus promotion, source publication,
GitHub Release and the announcement remain part of the release procedure.

A final tag pushed with `GITHUB_TOKEN` would also suppress downstream push
workflows. The RC dispatch mechanism is not a final-release dispatcher, and Ruby
publishing still requires a final-tag push. Any future automation of final tags
must account for those conditions.

For ATR-staged sources, publish the approved candidate revision through ATR.
The SVN move commands in the release procedure apply to SVN-staged candidates.
Preserve the approved source archives through publication.
