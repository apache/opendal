---
title: Weekly source releases
sidebar_position: 4
---

# Weekly source releases

The weekly workflow prepares a version PR each Friday from the Friday 00:00 UTC
main cutoff. Merging that PR calls the existing
`release-compose.yml`, which builds, signs and uploads the candidate to ATR.
The release manager then follows the [release procedure](release.md) to verify
the candidate, start the vote and finish publication.

## Preparation

The workflow selects the latest recorded main push at or before the cutoff.
Push-run timestamps record arrival on main; commit dates do not. Keep that run
history until preparation finishes. The first week requires a recorded push before
the cutoff. Manual dispatch uses the same cutoff and starts a fresh attempt.

`update-version --patch` prepares at least a patch increment for each package from
the latest reachable final release tag, preserving higher inventory versions and
versions of new packages. Final tags must identify successfully published releases.
It reuses the existing compatibility validation and package update logic. Breaking
changes may require editing the inventory before running preparation again.

Each attempt creates a candidate branch and a version PR from its `-bump` branch.
Review versions and replace the generated changelog link with release notes as
needed. Before merging, check that no other candidate is being voted on or
published. The merge SHA becomes the RC tag and the input to the existing compose
workflow. The RC suffix identifies the Actions run and attempt.

If preparation or validation fails, start a new workflow run. Abandon the old PR,
branch and any draft artifacts as appropriate. An active vote must be canceled
before replacing its candidate; approved releases must finish publication.

## Configuration

Allow Actions to create PRs. Reuse the existing `GPG_SECRET_KEY` secret and
`SOURCE_SIGNING_FINGERPRINT` variable. Register the weekly workflow for ATR compose
OIDC, since the reusable workflow retains its caller's identity. The RM merging
the preparation PR must have the required ASF-linked project permissions.

The workflow ends at ATR compose. It does not start votes, announce releases,
create final tags, synchronize main or publish language packages. These remain
steps in the existing release procedure. Keep the candidate branch and signed
artifacts until that procedure is complete.

Installing this workflow on main activates the schedule. Repository PR permissions
and the reusable workflow's ATR OIDC identity still require a live rehearsal;
local checks and this PR do not establish a successful remote upload.
