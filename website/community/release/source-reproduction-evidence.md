---
title: Source archive reproduction evidence
sidebar_position: 7
---

# Source archive reproduction evidence

On 2026-09-07, all 12 OpenDAL source archive SHA-512 digests matched across two
Linux CI checkouts, two macOS CI checkouts and two local macOS checkouts outside
GitHub Actions. No working-tree patch was applied in any run.

## Source identity

- PR: [#8232](https://github.com/apache/opendal/pull/8232).
- PR head: `114e89f50614b2e02a747db548dc61537123a625`.
- Base: `a3e2bd4b6944517ac897c7476de7bfdd9a66d897`.
- **Actual archived commit**: `50b8ed2ead3405ed266cdc1c495cf48a920ee54b`.
  GitHub's pull-request workflow checked out this synthetic merge commit. The
  local rebuild used this same commit, not just the PR branch head.
- [Successful CI run](https://github.com/apache/opendal/actions/runs/34069111577).
- [Linux evidence artifact](https://github.com/apache/opendal/actions/runs/34069111577/artifacts/9999898144).
- [macOS evidence artifact](https://github.com/apache/opendal/actions/runs/34069111577/artifacts/9999894862).

CI artifacts contain `report.json` and build logs, not the source tarballs. The
comparison below uses the CI-reported digests of the generated tarballs against
actual local tarball digests. It is not a signature verification or a comparison
against downloaded CI tarball files. Actions evidence is subject to retention;
the shared filename/hash inventory is recorded below for durable reference.

## Local reproduction

The local run used the verification script from PR head `114e89f50614`, executed
on the maintainer's local macOS host, outside GitHub Actions. An AI assistant ran
the commands; this report does not represent a human release vote or Security
approval.

```shell
git fetch origin 50b8ed2ead3405ed266cdc1c495cf48a920ee54b
python3 scripts/verify_source_reproducibility.py \
  --revision 50b8ed2ead3405ed266cdc1c495cf48a920ee54b \
  --output /tmp/opendal-trusted-rebuild-34069111577
```

For each of the three reports, the comparison checked the commit, empty patch
digest, successful result, and equality of both complete filename/hash maps with
the Linux report. Different source mtimes, filesystem permissions and timezones
were used in the two checkouts on each host. Every artifact below matched in all
six builds. The platform-specific reproduction logs remain in the linked CI
artifacts; local logs and archives are retained with the local report.


To repeat the report comparison after downloading the CI evidence:

```shell
gh run download 34069111577 --repo apache/opendal \
  --pattern 'source-reproducibility-*' --dir /tmp/opendal-ci-evidence
```

```python
import hashlib
import json
from pathlib import Path

paths = sorted(Path("/tmp/opendal-ci-evidence").glob("*/report.json"))
assert len(paths) == 2
paths.append(Path("/tmp/opendal-trusted-rebuild-34069111577/report.json"))
reports = [json.loads(path.read_text()) for path in paths]
expected = reports[0]["runs"][0]
assert len(expected) == 12
for report in reports:
    assert report["commit"] == "50b8ed2ead3405ed266cdc1c495cf48a920ee54b"
    assert report["patch_sha512"] == hashlib.sha512(b"").hexdigest()
    assert report["matched"] and len(report["runs"]) == 2
    assert all(run == expected for run in report["runs"])
```

| Environment | Rust compiler | Target |
| --- | --- | --- |
| Linux CI | `rustc 1.98.1 (48a229cea 2026-09-01)` | `x86_64-unknown-linux-gnu` |
| macOS CI | `rustc 1.98.1 (48a229cea 2026-09-01)` | `aarch64-apple-darwin` |
| Local macOS | `rustc 1.97.0 (2d8144b78 2026-07-07)` | `aarch64-apple-darwin` |

## Matching archive inventory

| Archive | SHA-512 (all six builds) |
| --- | --- |
| `apache-opendal-bindings-c-0.47.4-src.tar.gz` | `99f747ef177e2904c8a68aa031037772c970fdd8a3870b4c4697bcefae2c5cff3b33863b45ea77e1c44db5265c8cf0613ed399ef9f29ef5f36a7a3b15968f005` |
| `apache-opendal-bindings-cpp-0.45.31-src.tar.gz` | `2cd94d46de131e662a5de8ff5e55c7976b7fd0588c4029b7ccdc27d0ceb608509d9799c008201c1df813545b4261530982a7733c542862ac637b468471caadca` |
| `apache-opendal-bindings-dotnet-0.2.1-src.tar.gz` | `905330b6a3fb06d15192b20fb1459d8fdbb5e60e7897409fac7ae65aa5a1d18503d4e96ff8a9f808c259a9fc0edbfecb29868ddd5361e26271f7700de7674e49` |
| `apache-opendal-bindings-java-0.50.4-src.tar.gz` | `352b1b1b6b8cb6a91c6e64dbb432ae8b222eb692c991286dfd38107bf057122cf5d4005ac473244231ecdd5c02fdd42655e7e86950a3027408b65324ecffee74` |
| `apache-opendal-bindings-nodejs-0.49.9-src.tar.gz` | `32e2158f2283c4cff7706f32a5334083e8fc1794e57431152096cc73fa6a6ffa66cbe60228495ab51f1697a4344794b59c22169b3698414f8253fe727501640c` |
| `apache-opendal-bindings-python-0.47.8-src.tar.gz` | `38c2c62f9f78e2f3a844c97d11f3eec48b294531ecc9eaa4f17529201b6d474eb3cc2593faa891b976d8b50b8cd2a37eb7e1d5f995aa283552b3e0cf7d00923a` |
| `apache-opendal-bindings-ruby-0.1.12-src.tar.gz` | `2c7265208864e6e918e4d087ed300445daa33bcd928d52459b0df7566b93d1015d9638fc5f0b06a7736c77470d7c82a8fbabaab4154bb9fbc2f67f1f4dd9d011` |
| `apache-opendal-core-0.59.1-src.tar.gz` | `2df1535503b71dcb913c1e83b9e5ff3dbbd3d9ad5921f3521ad4bc8e282ed0c95ea0f680425f848aebe62e19d15692ee5d0dd552d9a34376811d23e6892bd6f1` |
| `apache-opendal-integrations-dav-server-0.7.7-src.tar.gz` | `f2e857c6505ad34d3d1b2a7bbe75c42fd03c970d9bd6812725950c535e452a3f63d05feb33ed8f969bd35ce07708146a2bfbc97d4bf6f7788534b9f1a40da489` |
| `apache-opendal-integrations-object_store-0.60.1-src.tar.gz` | `6735b49de404c77b22124ec99d1e66d31c8f816de0d51d9aa134da6ad671a9e9cfaa245fc84169c85c8dafe33ac92607e8922047dedca8c73698f9d72a977875` |
| `apache-opendal-integrations-parquet-0.10.1-src.tar.gz` | `cd025016e38bad333d02b86eae9f0112868c7287f3dc754a9e8371a37c3c529ea92f2eb8f34641509042e3b31eaf22533842437e2b88bd30e4df892d1bc2d81a` |
| `apache-opendal-integrations-unftp-sbe-0.4.7-src.tar.gz` | `ad5b6282715ac0ae57da21e88c6319e5ec793b849e0ff561ae568a59dfe7ac40792f07923f5adae0c0e6fc77125c70d3709dc1f84e7dc0087ca52e6527102679` |

## Interpretation

This demonstrates source archive reproducibility for the identified commit and
packaging dependencies. It does not cover binary artifacts, OpenPGP signatures,
future dependency changes, or the content of a subsequent release candidate.

The follow-up PR changes that add this report and the signing proposal are
website documentation only, outside the source archive inventory. They do not
change the tested packager or source payloads. Actual releases must repeat local
reproduction against the voted-on staged files, including signature verification.
