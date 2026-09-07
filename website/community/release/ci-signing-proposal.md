---
title: Source archive CI signing proposal
sidebar_position: 6
---

# Source archive CI signing proposal

This proposal requests review of automated OpenPGP signing for OpenDAL source
`.tar.gz` release archives in `apache/opendal`. It does not enable signing, request
credentials, introduce a new workflow, or change the release cadence. ASF Security
review and Infra provisioning are prerequisites for implementation.

## Scope and integration point

Sign only the source archives enumerated by `dev/src/release/package.rs`, after
`odev release --unsigned` finishes and the complete artifact inventory and hashes
are validated. This currently covers 12 core, integration and binding source
archives. Generate one ASCII-armored detached `.asc` per archive; retain its
`.sha512` file. Do not sign Git tags, approval documents, wheels, JARs, registry
packages, arbitrary uploaded files, or executable build tools with this key.

The proposed integration extends the existing source packaging and reproduction
path in `.github/workflows/ci_odev.yml`. Normal pull-request and push checks remain
unsigned. A later implementation would add a release-manager-initiated run from
trusted `main`, identifying the exact reviewed candidate commit, and a separately
protected signing job after packaging. No release signing step is added by this PR.

Candidate code runs only in the unsigned build job. The signing job downloads that
run's frozen artifact bundle and verifies the inventory and SHA-512 manifest using
trusted tooling. It must not execute candidate-provided scripts while holding the
key. The job's reviewed implementation, allowed refs, environment protection and
secret scope must be agreed with Security and Infra before activation. Neither a
PR run nor a tag push by itself authorizes use of the signing key.

## Human verification and publication

The release manager downloads the signed candidate and uploads the same bytes to
ASF SVN `dist/dev/opendal` using local credentials. The application should explicitly
ask Security whether this manual staging handoff is acceptable under the automated
signing guidance's CI staging requirement; it must not be presented as an already
approved arrangement. This proposal requests no CI SVN credentials and does not
require ATR.

Before official publication, a release reviewer independently rebuilds every
candidate source archive from the exact commit on trusted hardware outside GitHub
Actions. The reviewer compares the entire filename set and SHA-512 digests with
the staged candidate, verifies OpenPGP signatures against OpenDAL KEYS, and records
the evidence in the release vote. A missing or mismatching artifact blocks release.
CI-to-CI agreement is useful regression coverage but is not this human verification.

The release manager reviews reproduction evidence, source/license verification,
unresolved objections and the PMC vote before approving publication. The signing
job cannot publish stable tags or packages, write ASF `dist/release`, update the
public download page, or send a release announcement. Signing failure leaves an
incomplete candidate; it cannot silently fall back to unsigned publication.

## Evidence for the application

See [Source archive reproduction evidence](./source-reproduction-evidence.md) for
an exact-commit comparison between Linux CI, macOS CI and a local macOS rebuild.
The checks use [the documented reproduction procedure](./reproducible-source.md).
This is evidence for the source packaging implementation, not an approved release
vote or a claim about binary reproducibility. Each actual release still requires
its own trusted-hardware comparison against the staged files.

## Proposed Security review request

Subject: Review request: automated signing of Apache OpenDAL source archives

We would like to request review of automated signing for Apache OpenDAL's source
release archives. The proposed key would sign only the `.tar.gz` files produced
by our source packager, not convenience binaries or Git tags.

PR #8232 normalizes archive metadata and adds independent-checkout and cross-platform
reproduction checks. The linked evidence compares all 12 archives from one exact
commit across Linux CI, macOS CI and a local machine outside GitHub Actions.

We propose to place signing after unsigned source packaging in the existing CI
path, using a protected job that does not execute candidate code with the key.
Pull-request checks would remain unsigned. Before each official release, a reviewer
would independently reproduce all staged archives on trusted hardware, compare
their hashes, verify signatures and record the results for the PMC vote.

The release manager would initially upload the signed candidate to ASF SVN dev
using local credentials. Please confirm whether this manual staging handoff is
acceptable, and what workflow and environment restrictions you require before
Infra provisions the project key. We will submit the concrete signing job for
review before enabling it. No key or remote signing configuration is requested by
this PR itself.

## Infra request after Security review

Open an Infra Jira request referencing Security's review and the final workflow.
Identify `apache/opendal`, the source-only artifact inventory, the agreed workflow
and protected job, and the verified reproduction procedure. Ask Infra to generate
a project signing-only key, provision it to the approved CI scope, and provide the
public key for OpenDAL KEYS. Secret names and any passphrase handling must be agreed
with Infra rather than assumed by the implementation. Do not supply a personal
release-manager private key.

The [ASF automated signing procedure](https://infra.apache.org/release-signing.html#automated-release-signing)
specifies a 4096-bit RSA signing-only key, Infra custody of the private key, and
PGP-encrypted revocation material in the project's private repository. Security
approval is needed before the workflow is put into use. Confirm these provisions
in the request; this document does not assert that approval has been granted.
