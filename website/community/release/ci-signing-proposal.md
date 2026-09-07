---
title: Source archive trusted publishing
sidebar_position: 6
---

# Source archive trusted publishing

The manually dispatched `release-compose.yml` workflow prepares signed source
candidates on Apache Trusted Releases (ATR). It remains disabled until ASF Security
approves the workflow and the live GitHub/ATR configuration is verified. Normal CI
continues to build unsigned archives. This workflow does not schedule releases,
start votes, finish releases, or publish language packages.

## Workflow boundary

Dispatch the workflow from `main` with a full reviewed candidate commit SHA and an
`X.Y.Z-rc.N` candidate version. Initially, the candidate must be reachable from
`main`, and its core package version must equal `X.Y.Z`. Candidate branches and
automatic version selection require a separate extension.

Three jobs separate credentials and responsibilities:

1. The build job runs the reproduction tool on the candidate in two independent
   checkouts. It retains the complete unsigned source bundle and report as an
   immutable Actions artifact. It has neither signing secrets nor OIDC permission.
2. The protected `source-release` job downloads that artifact by ID, validates its
   commit, reproduction report, package inventory and SHA-512 checksums, and signs
   only the expected `.tar.gz` files. Trusted tooling comes from the workflow's
   `main` checkout; candidate scripts are not executed with the key. The public
   key must already be in OpenDAL KEYS. Each generated signature is verified
   against the configured primary fingerprint before the signed bundle is saved.
3. The upload job downloads the signed bundle by ID and uses the commit-pinned
   `apache/tooling-actions/upload-to-atr` action. GitHub OIDC authorizes a temporary
   SSH key, and rsync transfers the bundle to ATR compose. This job receives
   `id-token: write`, but no GPG private key. No SVN credentials or persistent ATR
   token are needed.

The signed bundle contains only source `.tar.gz`, `.sha512` and `.asc` files.
Build logs and reproduction reports stay in Actions. An upload success does not
mean ATR checks passed: inspect the candidate revision and check results in ATR.
The upstream upload action is experimental; its pinned implementation must be
reviewed again before upgrading.

## Enablement

1. Send ASF Security the workflow and the reproduction evidence in
   [PR #8232](https://github.com/apache/opendal/pull/8232). Explain that reviewers
   will independently rebuild the actual staged archives on trusted hardware
   outside Actions before publication. Obtain workflow approval before use.
2. Reuse the Infra-managed `GPG_SECRET_KEY` repository secret originally
   provisioned in [INFRA-24880](https://issues.apache.org/jira/browse/INFRA-24880).
   The existing Java release workflow uses this key with an empty passphrase;
   source signing uses the same configuration. Do not export or replace the
   private key. No new key or secret is requested.
3. Verify that the existing public key is available in OpenDAL KEYS and associated
   with the committee in ATR:
   - UID: `ASF OpenDAL Services RM <private@opendal.apache.org>`.
   - Primary fingerprint: `F70370C26871BFCC47D121A626143ED2AE57525E`.
   Set `SOURCE_SIGNING_FINGERPRINT` to this fingerprint. ATR recognizes the legacy
   `Services RM` naming convention. Preserve the existing KEYS management mode.
   The historical JAR signing approval does not by itself document approval of
   this source archive workflow.
4. In the OpenDAL project's ATR settings, under Trusted Publishing, configure:

   | Setting | Value |
   | --- | --- |
   | Repository name | `opendal` |
   | Repository branch | `main` |
   | Compose workflows | `.github/workflows/release-compose.yml` |
   | Vote workflows | Leave empty |
   | Finish workflows | Leave empty |

5. Confirm that the `source-release` environment protection from `.asf.yaml` is
   active, including main-only deployment and required reviewers. A YAML change
   alone is not evidence that GitHub applied the protection. The signing key is a
   repository secret under the documented Infra procedure, so also restrict and
   review changes to workflows that could access repository secrets.
6. Set the repository variable `SOURCE_RELEASE_ENABLED=true` only after these
   prerequisites are verified. Leave it unset to keep compose disabled.

The repository secret's existence and the public key in KEYS have been verified.
An authenticated end-to-end source upload and the live ATR configuration have not
yet been verified by this PR. No Security or Infra request is sent by
the workflow.

## Rehearsal and release handoff

For a rehearsal, use a candidate version and run the real compose workflow. Inspect
ATR checks, download the complete staged revision, and use the
[reproduction procedure](./reproducible-source.md) to compare every source archive.
Verify the signatures against KEYS. Stop in compose: do not start a vote or finish
the release. This exercises staging without publishing an official release; it
still creates real candidate data in ATR. If signing or upload fails, treat the
candidate as incomplete and inspect ATR before retrying. Reuploads can create a
new ATR revision; the workflow does not automatically delete or approve revisions.

For a real release, reviewers perform the same independent rebuild on trusted
hardware and record the exact candidate revision and results for the PMC vote.
Link the vote to ATR's pinned candidate revision. CI-to-CI agreement alone does
not replace that verification. After a passing vote, a release manager can use
ATR's finish page to publish the approved files. ATR handles the distribution SVN
write; this compose workflow has no finish permission. Check the destination
layout before publication so it matches OpenDAL's release directory conventions.

## References

- [ATR Trusted Publishing](https://releases.apache.org/docs/trusted-publishing)
- [ATR staging and voting](https://releases.apache.org/docs/staging-and-voting)
- [ATR publication and KEYS management](https://releases.apache.org/docs/promoting-to-release)
- [ASF automated signing procedure](https://infra.apache.org/release-signing.html#automated-release-signing)
- [Official upload action](https://github.com/apache/tooling-actions/tree/fa721a0b176d713807b574da721b96545b587eea/upload-to-atr)
