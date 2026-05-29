<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache OpenDAL Security Threat Model (draft)

## §1 Header

- **Project:** Apache OpenDAL (`apache/opendal`)
- **Commit:** `ecd52fbf` (HEAD of `main` at draft time)
- **Date:** 2026-05-29
- **Author:** ASF Security team, first-pass draft from public artefacts
- **Status:** Draft — awaiting maintainer review
- **Version binding:** This document describes the model as of the commit above. A
  vulnerability report against OpenDAL version *N* should be triaged against the
  model as it stood at *N* (release tag), not against HEAD.
- **Reporting cross-reference:** Findings that may violate a §8 property should
  be reported to `private@opendal.apache.org` per
  [`website/community/security.md`](website/community/security.md) before any
  public disclosure. Findings that fall under §3 or §9 will be closed citing
  this document.
- **Provenance legend:** *(documented)* — paraphrased from a public OpenDAL
  artefact with citation; *(maintainer)* — confirmed by an OpenDAL maintainer
  in response to this draft; *(inferred)* — synthesised from code/structure
  but not yet ratified, every such tag has a matching §14 question.
- **Draft confidence:** 19 documented / 0 maintainer / 32 inferred.

**About the project.** Apache OpenDAL ("Open Data Access Layer") is a Rust
library that gives applications a single `Operator` API to read, write, list,
and stat data across heterogeneous storage backends — object stores (S3, GCS,
Azure Blob, OSS, COS, B2, …), POSIX file systems, HDFS-family file stores,
WebDAV / FTP / SFTP, cloud SaaS (Google Drive, Dropbox, OneDrive, Hugging
Face, GitHub), relational and key-value databases, and in-process key-value
stores. Each backend lives behind the same `Access` trait and the same
`Operator` facade; orthogonal behaviour (retry, timeout, logging, tracing,
metrics, throttling, content-type inference, route, in-memory immutable
index, capability check) is composed in via `Layer`s. OpenDAL exposes the
same model to ~15 language bindings (C, C++, Python, Java, Node.js, Go, Ruby,
.NET, OCaml, PHP, Haskell, Lua, Swift, Dart, D, Zig), all of which build on
the Rust core or its C ABI shim. The pilot scope of this threat model is the
`apache/opendal` repository at the commit above; the three satellite repos
(`opendal-reqsign`, `opendal-fastrace-jaeger`, `opendal-website`) are
out-of-scope here and will be modelled separately.

## §2 Scope and intended use

**Primary intended use.** *(documented — README, `core/core/src/docs/concepts.rs`)*
OpenDAL is an in-process library embedded by a host application that wants
backend-uniform storage access. The model is "a host application instantiates
a `Builder`, finalises it into an `Operator`, optionally wraps it with one or
more `Layer`s, and then issues per-path `read`/`write`/`list`/`stat`/`delete`
calls". OpenDAL is not itself a service: there is no listening socket,
no per-user authn/authz, and no rendering of results to an end user.

**Deployment contexts.** *(documented — README "Why OpenDAL"; AGENTS.md)*
OpenDAL is designed to be a dependency of long-running data systems (analytic
query engines, time-series databases, build caches, observability pipelines,
backup tools — see `core/users.md`). It is also shipped as language bindings
for in-process use from Python/Java/Node etc. It is NOT designed as a
gateway, daemon, or multi-tenant access broker; historical "oay" gateway
code has been removed from the repo (`AGENTS.md`: *"oay: removed from this
repository"*).

**Caller expectations.** *(inferred — Q14.1)* The caller is trusted to:
choose the backend type, supply credentials and endpoints, pick the `root`
under which the `Operator` is scoped, decide which layers to apply, and
decide which user-supplied bytes ever reach OpenDAL as a *path*. OpenDAL has
no notion of "principal"; the same `Operator` instance has the same
authority for every call.

**Component-family table.** This is the most load-bearing orientation table
for triage.

| Family | Representative entry | Touches outside the process? | In this model? |
| --- | --- | --- | --- |
| Core `Operator` / `Access` trait | `opendal::Operator::read/write/list/stat` | no (only through a Service) | yes |
| In-memory / in-process services (`memory`, `dashmap`, `moka`, `mini-moka`) | `services::Memory` | no | yes |
| Local-FS services (`fs`, `compfs`, `monoiofs`, `opfs`) | `services::Fs` | **yes — filesystem** | yes |
| Object-store services (`s3`, `gcs`, `azblob`, `oss`, `cos`, `obs`, `b2`, `r2` via s3, `tos`, `swift`, `azdls`, `azfile`, `upyun`, `vercel-blob`, `lakefs`, `ipfs`, `ipmfs`) | `services::S3` | **yes — network + creds** | yes |
| HDFS-family services (`hdfs`, `hdfs-native`, `webhdfs`, `alluxio`, `goosefs`, `dbfs`) | `services::Hdfs` | **yes — network** | yes |
| SaaS services (`gdrive`, `dropbox`, `onedrive`, `huggingface`, `github`, `aliyun-drive`, `pcloud`, `koofr`, `seafile`, `yandex-disk`) | `services::Gdrive` | **yes — network + OAuth** | yes |
| Generic protocol services (`http`, `ftp`, `sftp`, `webdav`) | `services::Http` | **yes — network** | yes |
| Database/KV services (`mysql`, `postgresql`, `sqlite`, `redis`, `mongodb`, `surrealdb`, `d1`, `cloudflare-kv`, `gridfs`, `etcd`, `memcached`, `tikv`, `foundationdb`, `rocksdb`, `redb`, `persy`, `sled`, `cacache`, `ghac`, `vercel-artifacts`) | `services::Postgresql` | **yes — network or local FS** | yes |
| Layers (retry, timeout, logging, tracing, metrics, throttle, concurrent-limit, mime-guess, route, immutable-index, capability-check, foyer, chaos, dtrace, async-backtrace, …) | `op.layer(RetryLayer::new())` | no (instrument only) | yes |
| Language bindings under `bindings/` (C, C++, Python, Java, Node.js, Go, Ruby, .NET, OCaml, PHP, Haskell, Lua, Swift, Dart, D, Zig) | `import opendal` | inherited from core | yes — see §3 caveats |
| Repo integrations under `integrations/` (`dav-server`, `object_store`, `parquet`, `spring`, `unftp-sbe`) | `dav-server-opendalfs` | **yes — these add a listener** | **see §3 caveats** |
| `examples/`, `core/edge/`, `core/benches/`, `fixtures/` | `examples/cpp/`, `core/edge/*` | varies | **out — §3** |

A finding is in-model only if it lands in a row marked "yes" — see §4 for
the per-component reachability test.

## §3 Out of scope (explicit non-goals)

- **Storage-provider authorisation.** *(inferred — Q14.2)* Whether a bucket
  policy, IAM role, ACL, POSIX mode bit, or HDFS permission permits a given
  operation is decided by the storage system, not by OpenDAL. A misconfigured
  S3 bucket policy that allows public read is not an OpenDAL vulnerability.
- **Server-side correctness of the backend.** *(inferred — Q14.2)* OpenDAL
  trusts what the storage service returns. A backend returning crafted
  bytes, mismatched ETags, wrong sizes, or moved data is treated as a
  trusted control-plane error, not as an OpenDAL trust-boundary violation.
- **Caller-side authn/authz over end-users of the embedding application.**
  *(inferred — Q14.1)* The embedding application owns its user model.
  OpenDAL does not know about end-user identities.
- **Network transport security configuration.** *(inferred — Q14.3)* TLS
  certificate validation, CA bundles, proxy trust, and HTTPS-vs-HTTP choice
  are properties of the HTTP client and the host environment. OpenDAL
  inherits whatever the `reqwest` HTTP client (or a caller-supplied
  `HttpClient`) does by default; downgrading or weakening transport is the
  operator's choice.
- **Path-confinement of the `services::Fs` backend below its `root`.**
  *(inferred — Q14.4 — high-priority)* `FsCore` builds the absolute path as
  `self.root.join(path)` (`core/services/fs/src/core.rs`). `PathBuf::join` does
  not resolve `..` and does not refuse symlinks that escape `root`. A caller
  that passes attacker-controlled path strings into the `Fs` backend can read
  / write outside `root`. The model's position is that **the caller is
  responsible for refusing untrusted path components before they reach the
  `Operator`**; symlink-escape and `..`-traversal against `Fs` are *not*
  OpenDAL vulnerabilities by default.
- **The repo's integration layer when it exposes a listener.** *(inferred —
  Q14.5)* `integrations/dav-server` (a WebDAV server backed by `OpendalFs`)
  and `integrations/unftp-sbe` (an FTP server backend) take the unscoped
  OpenDAL `Operator` and make it network-reachable. Authentication and
  authorization on those listeners are the responsibility of the surrounding
  `dav-server` / `unftp` host process, not of OpenDAL. The integrations
  themselves are in-model; the *deployment shape* in which they are exposed
  to untrusted network peers without an upstream authn layer is not.
- **`examples/`, `core/edge/`, `core/benches/`, `fixtures/`.** *(documented —
  these directories carry runnable demos / regression edges / micro-benchmarks
  / golden test data, not production-grade code.)*
- **Three satellite repos.** `opendal-reqsign` (request-signing), 
  `opendal-fastrace-jaeger`, and `opendal-website` are explicitly out of this
  pilot model and will be modelled separately.
- **Build / release / supply-chain hygiene.** GitHub Actions pinning, release
  signing, dependency freshness, MSRV — out of model per the SKILL.
- **Side channels.** *(inferred — Q14.6)* OpenDAL provides no timing,
  cache, or co-tenant side-channel resistance.

## §4 Trust boundaries and data flow

The boundary is **the public API surface of the `Operator`**. Bytes that the
caller passes into `read/write/list/stat/delete` — both the path and any
data payload — are treated as already-authenticated by virtue of the
caller having presented them. Bytes that come *back* from a Service (object
body, metadata, listing pages) are treated as control-plane content from a
trusted backend.

There are **three additional internal boundaries** OpenDAL itself is
responsible for:

1. **Per-`Operator` credential isolation.** *(inferred — Q14.7)* An
   `Operator` carries the credentials, endpoints, and config of the
   `Builder` it was finished from. Two distinct `Operator`s in the same
   process must not cross-contaminate each other's credentials, in-memory
   token caches, or signers.
2. **Layer composition isolation.** *(inferred — Q14.7)* Layers wrap a
   single `Operator`. A layer must not leak per-operation state (paths,
   request bodies, error contexts containing credentials) across
   independent `Operator` instances or across concurrent calls on the same
   `Operator`.
3. **Debug / log redaction of credentials.** *(documented — RFC 0203
   `core/core/src/docs/rfcs/0203_remove_credential.md`, and PR #7523
   "redact credentials in debug output", `core/CHANGELOG.md`)* Builders
   manually implement `Debug` to redact sensitive fields. The
   `LoggingLayer` emits structured per-operation log lines but is expected
   not to surface credentials or signer state.

**Reachability preconditions per component.**

- A finding against a service (e.g. `services::S3`) is in-model only if it is
  reachable from a normal call sequence — `Builder` → `Operator` →
  `read`/`write`/etc. — using the documented configuration knobs in §6.
  Reachable-only-via-`raw::Access`-internals findings against a service are
  in-model only if they violate one of the three boundaries above.
- A finding against `services::Fs` is in-model only if it is **not** reducible
  to "the caller passed an attacker-controlled `path` argument". `..` / symlink
  escape via the path argument is out of model per §3.
- A finding against a Layer is in-model only if it (a) leaks credentials or
  internal control state through its outward interface (log line, metric
  label, span attribute, error message), or (b) violates the cross-`Operator`
  isolation property in boundary 1.
- A finding against a Binding (`bindings/python`, `bindings/java`, …) is
  in-model only if it diverges from the core's contract in a way that
  produces a new trust-boundary violation, not merely a binding-layer crash
  on malformed input.

## §5 Assumptions about the environment

- **OS / runtime.** *(documented — AGENTS.md)* MSRV is Rust 1.85. Core
  supports Linux, macOS, Windows; some services (e.g. `monoiofs`,
  `compfs`, `opfs`) target specific runtimes. POSIX `fs` assumes a POSIX-like
  file system; `opfs` targets browser Origin Private File System via WASM.
- **Concurrency.** *(documented — `core/core/src/docs/concepts.rs`)* `Operator`
  is `Send + Sync` and is cheap to clone (`Arc` inside). The library is
  fully async over a Tokio-style runtime; blocking wrappers exist.
- **Memory.** *(inferred — Q14.8)* Rust safe-code memory model. `unsafe` is
  used at FFI / binding boundaries (`bindings/c`, `bindings/python`,
  `bindings/java`, etc.) and where service implementations need it for
  performance.
- **Time.** *(documented — `core/core/src/docs/upgrade.md` v0.55)* All
  timestamps are `jiff::Timestamp`. The library does not require monotonic
  clocks for security-relevant purposes; signer freshness is delegated to
  `reqsign` (out-of-pilot satellite repo `opendal-reqsign`).
- **Filesystem, network, env.** *(documented and inferred — Q14.9)*
  - The `fs` family reads/writes the local filesystem under the configured
    `root`. *(documented — `core/services/fs/src/core.rs`.)*
  - Object-store and SaaS services open outbound HTTPS connections to
    user-configured endpoints. *(documented — service `config.rs` files.)*
  - Several services read credentials from the environment by default
    (e.g. S3 reads `AWS_ACCESS_KEY_ID` etc., unless `disable_config_load` is
    set; GCS reads GCE metadata unless `disable_vm_metadata` is set).
    *(documented — `core/services/s3/src/config.rs`, `core/services/gcs/src/config.rs`.)*
  - The library writes log entries via the `log` crate when `LoggingLayer`
    is in effect, and emits tracing spans when `TracingLayer` is in effect.
    Otherwise it is quiet on stdout/stderr.
- **What the project does NOT do to its host.** *(inferred — Q14.9, high
  priority — predominantly negative claims)*
  - No installation of process-wide signal handlers (no `SIGPIPE`, no
    `SIGCHLD`).
  - No spawning of child processes from the core.
  - No mutation of the global allocator, locale, or FPU state.
  - No global panic hook installation.
  - No silent fallback to plaintext when HTTPS is configured.
  - No persistence of credentials to disk (token caches, if any, live in
    process memory for the lifetime of the `Operator`).

## §5a Build-time and configuration variants

OpenDAL is a *family* of binaries: each service is feature-gated, and many
services expose runtime knobs that change the security envelope. The most
load-bearing knobs:

| Knob | Service(s) | Default | Effect on the model | Maintainer stance |
| --- | --- | --- | --- | --- |
| `skip_signature` | s3, gcs, oss, cos, tos, azblob | `false` | When `true`, the service issues unsigned requests, leaking neither a credential nor an auth header. Caller is asserting the bucket is public-read or that signing happens upstream. *(documented — `core/services/*/src/config.rs`, upgrade.md v0.57)* | *(inferred — Q14.10)* Supported; the documented production knob for accessing public buckets without holding a credential. |
| `allow_anonymous` | s3, gcs, oss, cos, tos, azblob | `false` | Deprecated alias for `skip_signature`. *(documented — upgrade.md v0.57)* | Deprecated; use `skip_signature`. |
| `disable_config_load` | s3, gcs, …  | `false` | When `true`, the service does **not** read AWS / GCS env vars or config files. Caller must supply creds explicitly. *(documented — `core/services/s3/src/config.rs`, `core/services/gcs/src/config.rs`.)* | *(inferred — Q14.10)* Supported; used by callers who want hermetic, non-ambient credentials. |
| `disable_ec2_metadata` | s3 | `false` | When `true`, suppress IMDS lookup. *(documented — `core/services/s3/src/config.rs`; upgrade.md historical note re: confusion with `disable_config_load`.)* | *(inferred — Q14.10)* Supported. |
| `disable_vm_metadata` | gcs | `false` | When `true`, suppress GCE metadata lookup. *(documented — `core/services/gcs/src/config.rs`.)* | *(inferred — Q14.10)* Supported. |
| Service feature flags (`services-s3`, `services-fs`, …) | all | per-service crate, off by default in the meta crate | Each disabled service is *not compiled in*. A finding against `services::Foo` is out of model in any build that does not enable `services-foo`. *(documented — AGENTS.md, `core/Cargo.toml`.)* | Supported. |
| `RetryLayer`, `TimeoutLayer`, `ConcurrentLimitLayer`, `ThrottleLayer` | core layers | not applied by default | Without these, OpenDAL makes no resource-bound guarantees on backend latency or concurrent in-flight ops. *(documented — README "Choose Your Layers".)* | *(inferred — Q14.11)* The maintainer position is "OpenDAL is not a rate-limiter or DoS shield by default; if you need bounded behaviour, apply the corresponding layer." |
| `atomic_write_dir` | fs | `None` | When set, writes go via a tempfile in this directory and are renamed into place. When `None`, writes go straight to the destination path. *(documented — `core/services/fs/src/config.rs`, `core/services/fs/src/core.rs`.)* | *(inferred — Q14.10)* Supported. |
| Service-specific `endpoint` knobs | s3, gcs, azblob, oss, … | service-default URL | A custom endpoint redirects all traffic. The caller is trusted to pick an endpoint. *(documented — `core/services/s3/src/config.rs`.)* | Supported; a malicious endpoint is out of model per §6. |

There is no `enable_unsafe` master flag and no debug-only auth-bypass mode
that ships disabled.

## §6 Assumptions about inputs

**Where inputs come from.** A real-world OpenDAL deployment receives:

- *Trusted from the embedding application:* the `Builder` configuration —
  credentials, endpoints, `root`, region, OAuth tokens, knobs in §5a.
- *Trusted from the embedding application:* the `path` argument to every
  operation, plus any `OpRead` / `OpWrite` / `OpList` options (range,
  if-match, content-type, user-metadata, cache-control, …).
- *Trusted from the embedding application:* the bytes written via `write`.
- *Trusted from the backend:* the bytes / metadata returned by
  `read` / `stat` / `list`, plus error responses.
- *Untrusted by anyone:* none. OpenDAL has no input source that is *both*
  untrusted *and* in-model. End-user input becomes in-model only after the
  embedding application has chosen to forward it through.

**Per-entry-point trust table.** Selected high-blast-radius entry points:

| Entry point | Parameter | Attacker-controllable in the model? | Caller must enforce |
| --- | --- | --- | --- |
| `Operator::new(Builder)` | `Builder` fields (creds, endpoint, region, root, knobs) | **no** — these are operator config | sanity-check endpoint URLs; do not source `Builder` fields from end-user input |
| `Operator::read(path)` | `path` | **no** — caller-supplied path | refuse `..` and symlink-escape components before calling; refuse paths sourced from untrusted users into a `services::Fs` backend rooted at a sensitive directory |
| `Operator::write(path, bytes)` | `path` | **no** | as above |
| `Operator::write(path, bytes)` | `bytes` | **yes within the model** — body is data | nothing — body is opaque to OpenDAL |
| `Operator::list(path)` | `path` | **no** | as above |
| `Operator::stat(path)` | `path` | **no** | as above |
| `Operator::delete(path)` | `path` | **no** | as above |
| `Operator::presign_read/write(path, expire)` | `path`, `expire` | **no** | scope of the presigned URL is the caller's responsibility |
| `Operator::from_uri(uri, options)` | `uri` (incl. scheme + root + query) | **no** — operator config | as above; the URI is config, not data |
| `services::Http::endpoint(url)` | `url` | **no** | the `http` service trusts its endpoint as a control-plane input |
| `services::Sftp::*` host/user | host, user, key | **no** | trusted operator config |
| Any `services::*` `endpoint`/`endpoint_url` | URL | **no** | trusted operator config |
| `LoggingLayer` | per-op events | **no** — sourced from caller paths + backend responses | install with awareness that paths and error messages will appear in log sinks |
| Service responses (e.g. S3 `GetObject` body, `ListObjectsV2` XML) | bytes / XML / JSON | **yes** (the backend may be wrong, but only the backend) | the body is treated as data; the response *envelope* (headers, status, parsed XML/JSON structure) is treated as control-plane and trusted |

**Size, shape, rate.** *(inferred — Q14.11)* OpenDAL provides no inherent
size, shape, or rate caps on inputs. Reads are streaming (via
`Reader` / `BufferStream`); writes are streaming. Lists are paginated through
the backend's native pagination. If the backend returns an unboundedly large
listing or an unboundedly long object, OpenDAL will forward that to the
caller. The caller is responsible for applying `TimeoutLayer`,
`ConcurrentLimitLayer`, `ThrottleLayer`, and / or its own buffering caps to
bound resource use.

## §7 Adversary model

**Who is the adversary.** The single in-scope adversary is **whoever can
influence the data bytes flowing through OpenDAL** without already being a
trusted operator of the embedding application — typically that means an
attacker who can influence what a backend returns, or, indirectly, an
attacker who can influence the bytes the caller decides to pass as `write`
payload.

**Capabilities the in-scope adversary has.**

- Can present arbitrary bodies in response to `read`. *(documented — this
  is the storage service's normal mode.)*
- Can present arbitrary `Content-Length`, `Content-Type`, `ETag`, and other
  response headers / metadata.
- Can stall, drop, or interleave responses (the backend or the network).
- Cannot supply the `path` argument or the `Builder` fields directly —
  those are operator-controlled. (See §3 for the explicit exclusion of
  the case where the embedding application *does* forward attacker bytes
  into the path.)

**Adversaries explicitly out of scope.**

- **An attacker who already controls the embedding process.** Such an
  attacker has trivially full control over the `Operator`, the `Builder`,
  and process memory; modelling them at the OpenDAL layer is meaningless.
- **An attacker who controls a *trusted* backend.** A malicious S3 endpoint
  configured deliberately by the operator is out of model — see §3 and
  the `endpoint` rows in §6.
- **A side-channel adversary** — co-tenant on the host, RowHammer-class,
  cache-timing, branch-prediction. *(inferred — Q14.6)*
- **A network adversary attacking the transport.** TLS is delegated to the
  underlying HTTP client; if the caller has chosen `http://` or
  weakened CA verification at the HTTP-client layer, the consequences are
  the caller's. *(inferred — Q14.3)*
- **An attacker who supplies path components.** This is the §3 case
  (`..` / symlink). The caller is documented as responsible.

The adversary model does **not** include the
"authenticated-but-Byzantine peer" of a consensus / replication system,
because OpenDAL is not a consensus system; it is a client of a backend that
provides its own correctness guarantees.

## §8 Security properties the project provides

Each property states what holds, the conditions, the violation symptom, and
a severity tier.

### 8.1 Credential redaction in `Debug` output and structured logs

- **Property.** Builders implement `Debug` by hand and redact credential
  fields; the `LoggingLayer` does not surface credentials in per-operation
  log lines.
  *(documented — RFC 0203 `core/core/src/docs/rfcs/0203_remove_credential.md`;
  PR #7523 "redact credentials in debug output"
  `core/CHANGELOG.md`; the redaction pattern is illustrated in the RFC.)*
- **Conditions.** Holds for all `Builder` types in the current tree.
  Out-of-tree services that implement their own `Builder` are responsible
  for their own `Debug`.
- **Violation symptom.** An `access_key_id`, `secret_access_key`,
  `session_token`, OAuth bearer token, or HTTP basic password appears in
  literal form in any of: a `format!("{:?}", builder)`, an `Error` chain
  via `format!("{:?}", err)`, a per-operation log line from
  `LoggingLayer`, a metric label, or a tracing span attribute.
- **Severity tier.** Security-critical (warrants CVE / coordinated
  disclosure).

### 8.2 Per-`Operator` credential isolation

- **Property.** *(inferred — Q14.7)* Two distinct `Operator` instances do
  not share signer state, OAuth token cache, or `reqsign` credential
  context across instances. A second `Operator` built from a different
  `Builder` does not see the first `Operator`'s credentials.
- **Conditions.** Single-process; both `Operator`s built via the public
  `Builder` API.
- **Violation symptom.** A signed request from `Operator` B carries
  credentials from `Operator` A, or a tracing/log line from `Operator` B
  surfaces credentials seeded into `Operator` A's `Builder`.
- **Severity tier.** Security-critical.

### 8.3 `path` normalisation is internal and well-defined

- **Property.** *(documented — `core/core/src/raw/path.rs`,
  `normalize_path` and `build_abs_path`.)* Paths are normalised: whitespace
  trimmed, leading `/` stripped, internal `//` collapsed, empty path
  becomes `/`. `build_abs_path` prepends the configured `root` (which must
  start and end with `/`).
- **Conditions.** All operations on `Operator`. For services that do not
  delegate to a remote API (`Fs`, `compfs`, `monoiofs`), the normalisation
  is the only sanitisation; see §3 for the explicit exclusion of `..`
  traversal.
- **Violation symptom.** Two distinct caller paths collide to the same
  backend path in a way that crosses caller intent, or `build_abs_path`
  produces a path that does not start with `root`.
- **Severity tier.** Correctness-only by default; security-critical iff it
  produces cross-caller data confusion within a single `Operator` (e.g.
  caller A's writes become readable to caller B through a backend-level
  collision).

### 8.4 Memory safety of the safe-Rust core

- **Property.** *(inferred — Q14.8)* The Rust core is memory-safe under
  well-formed inputs from the public API on the supported platforms,
  modulo the FFI `unsafe` blocks in `core/services/*` (notably anything
  that touches a C library: HDFS-JNI, FoundationDB, OCaml runtime,
  certain DB drivers) and in the language bindings.
- **Conditions.** Caller respects the API contract (size, lifetime,
  thread-safety annotations).
- **Violation symptom.** OOB read/write, use-after-free, double-free,
  data race observed by ThreadSanitizer / Miri / runtime crash on
  supported targets.
- **Severity tier.** Security-critical when reachable from the public API
  on a normally-configured backend; correctness when reachable only via
  internal `raw::*` use.

### 8.5 Layer composition is side-effect-free for credentials

- **Property.** *(inferred — Q14.12)* Applying any subset of the bundled
  layers in any order does not cause credentials to be emitted to the
  layer's outward channel. In particular, `LoggingLayer` + `TracingLayer`
  + `MetricsLayer` co-applied do not surface `Builder` credential fields
  in any of their outputs.
- **Conditions.** Layers are applied per the documented `op.layer(…)`
  API; layer ordering uses public combinators.
- **Violation symptom.** A credential appears in a tracing span attribute
  or a Prometheus label.
- **Severity tier.** Security-critical (it is a §8.1 violation routed
  through a layer).

### 8.6 No ambient privilege escalation from `disable_config_load = true`

- **Property.** *(documented — `core/services/s3/src/config.rs`,
  `core/services/gcs/src/config.rs`.)* When `disable_config_load = true` (or
  the more specific `disable_ec2_metadata` / `disable_vm_metadata`), the
  service does not read environment-supplied or IMDS-supplied credentials.
- **Conditions.** Service is `s3`-family or `gcs` with the explicit knob set.
- **Violation symptom.** A credential is loaded from the environment or
  IMDS despite the disable knob.
- **Severity tier.** Security-critical for the deployments that rely on
  hermeticity; correctness otherwise.

### 8.7 `skip_signature = true` does not also disable TLS / endpoint checks

- **Property.** *(inferred — Q14.13)* `skip_signature` controls only
  signing. It does not switch the transport to HTTP, does not weaken
  endpoint validation, and does not change the set of headers the service
  emits other than auth-related ones.
- **Conditions.** Service is an S3-family / GCS / OSS / COS / TOS / Azure-blob
  service with `skip_signature = true`.
- **Violation symptom.** Setting `skip_signature` causes a previously-HTTPS
  request to fall back to HTTP, or causes endpoint validation to be
  skipped.
- **Severity tier.** Security-critical.

### 8.8 Resource posture (explicit non-property)

OpenDAL **does not** make a categorical bound on memory, CPU, or wall-time
in input size. Streaming reads/writes are zero-copy where possible
(`Buffer`, `BufferStream` — `core/core/src/types/buffer.rs`), but neither
the body size of a `read` nor the number of entries in a `list` is
capped by the library. See §9.1 and §10 for the corresponding caller
responsibility.

## §9 Security properties the project does NOT provide

### 9.1 No resource-exhaustion defence by default

OpenDAL does not bound CPU, memory, or wall-time as a function of input
size or backend behaviour. A backend returning a 1 TB body to a `read`
call, or 10⁶ pages of `list` results, will be streamed through to the
caller. Apply `TimeoutLayer`, `ConcurrentLimitLayer`, `ThrottleLayer`,
and / or your own buffering caps. *(inferred — Q14.11)*

### 9.2 No authentication of backend bytes

OpenDAL does not MAC, sign, or otherwise authenticate object bodies. The
ETag returned by an object store is an integrity hint provided by that
backend, *not* a MAC. A caller that needs cryptographic integrity over
data at rest must add it above OpenDAL. *(inferred — Q14.14)*

### 9.3 No constant-time comparison anywhere

OpenDAL is not designed for secret comparison; no `Eq` impl in the public
API is documented constant-time. Do not compare credentials or HMACs by
calling OpenDAL APIs. *(inferred — Q14.15)*

### 9.4 No protection against path-traversal at the `Fs` boundary

`services::Fs` resolves `path` against `root` with `PathBuf::join` and
does not refuse `..` components or symlink escapes. Callers that source
path components from untrusted users **must** sanitise before calling.
*(documented — `core/services/fs/src/core.rs`; inferred maintainer position
— Q14.4.)*

### 9.5 No isolation between in-process `Operator`s built from the same backend

Two `Operator`s pointing at the same S3 bucket, or two `Operator`s rooted
at overlapping local directories, are *not* prevented from stepping on
each other. OpenDAL does not synthesise a per-`Operator` namespace beyond
what `root` and the backend's own naming provide. *(inferred — Q14.7)*

### 9.6 No defence against compression / decompression bombs

For services that gzip-encode responses (HTTP, some object stores), the
HTTP layer streams the decoded body to the caller. There is no
decompressed-output cap in OpenDAL. *(inferred — Q14.16)*

### 9.7 No defence against credential exfiltration via a malicious endpoint

If the caller configures an `endpoint` they do not control, the service
will sign and ship credentials at that endpoint. This is fundamental to
how AWS-SigV4 (and similar) work: the signature is bound to the URL,
which is operator-provided. *(documented — `core/services/s3/src/config.rs`
endpoint field; inferred maintainer position — Q14.2.)*

### 9.8 No cross-binding parity guarantee for security properties

Each language binding under `bindings/` has an independent version
(`README.md` note: *"Each binding has its own independent version
number"*). A property that holds for the Rust core at version *X* is
not guaranteed to hold for the Python binding at version *X* unless the
binding has been re-released against that core. *(documented — README
binding notes; inferred — Q14.17.)*

### 9.9 No supported "isolation" semantics for the `dav-server`/`unftp-sbe` integrations

The repo ships glue that lets a process expose an `Operator` over WebDAV
or FTP. Authentication and authorization on those listeners are the
host's job; OpenDAL does not contribute an authn layer there.
*(documented — `integrations/dav-server/README.md`,
`integrations/unftp-sbe/README.md`.)*

### 9.10 False-friend properties (call out separately)

- **`ETag` is not a MAC.** It is provider-defined and provider-trusted.
- **`Content-Length` is not authoritative.** OpenDAL uses it to size
  reads but does not refuse a body that exceeds it; mismatch is a backend
  bug, not an OpenDAL invariant.
- **`presign_read` / `presign_write` are not access-control primitives
  *for the caller*.** They are pre-authorised storage URLs whose scope is
  whatever the underlying backend grants; the caller is responsible for
  sizing the `expire` window and for not handing the URL to an unintended
  audience.
- **`skip_signature` is not "anonymous" — it is "send no signature".** The
  bucket policy on the backend still decides whether the request is
  allowed.
- **`disable_config_load` is not "no credentials" — it is "no *ambient*
  credentials".** Explicit credentials supplied to the `Builder` are
  still used.
- **The `LoggingLayer` is not an audit log.** It is an operator-facing
  observability tool; it is not designed to be tamper-evident.

### 9.11 Well-known attack classes left to the caller

- **Compression / decompression amplification** (gzipped HTTP response, gzipped
  S3 object) — §9.6.
- **Path traversal / symlink escape against `services::Fs`** — §9.4.
- **Server-side request forgery (SSRF) via a caller-controlled endpoint** —
  §9.7.
- **Untrusted backend XML / JSON parse amplification** — the response
  parsers are off-the-shelf (`serde-xml-rs`, `serde_json`, …); their
  amplification behaviour against malformed input is not bounded by
  OpenDAL. *(inferred — Q14.18)*
- **Log / metric label injection via attacker-controlled paths** — if
  `LoggingLayer` or `MetricsLayer` is connected to a sink that interprets
  labels structurally, attacker-controlled bytes in the path will land in
  the sink. *(inferred — Q14.19)*

## §10 Downstream responsibilities

The embedding application MUST:

1. Refuse `..` path components and symlink-escape paths before they reach
   a `services::Fs`-backed `Operator` rooted in a sensitive directory.
2. Treat the `Builder` configuration (creds, endpoints, region, root,
   §5a knobs) as operator-level config; do not source `Builder` fields
   from end-user input.
3. Apply `TimeoutLayer`, `ConcurrentLimitLayer`, and/or `ThrottleLayer`
   when callers can drive OpenDAL on untrusted bodies or list inputs and
   resource bounds matter.
4. Validate the integrity of backend bytes above OpenDAL if cryptographic
   integrity is required.
5. Not hand `LoggingLayer` / `MetricsLayer` output to a sink that
   interprets paths as structural labels without sanitisation.
6. Wrap `dav-server-opendalfs` / `unftp-sbe-opendal` in their own authn
   layer before exposing them to a network the operator does not control.
7. When taking a presigned URL from `Operator::presign_*`, treat it as a
   bearer credential and choose `expire` accordingly.
8. When relying on `disable_config_load = true` for hermeticity, audit
   that no other code in the same process is mutating
   `AWS_ACCESS_KEY_ID` or equivalent env vars at runtime.
9. Track binding versions independently: do not assume the Python binding
   at version *X* embeds Rust core at version *X*; consult the binding's
   `Cargo.toml` / changelog.

## §11 Known misuse patterns

- **Using `services::Fs` as a sandbox.** *(documented stance via §9.4,
  inferred — Q14.4)* The `root` parameter is a convenience, not a confinement.
- **Forwarding end-user paths directly to `Operator::*`.** Same root cause as
  above; applies in particular to web apps that turn URL path components
  into OpenDAL paths.
- **Calling `presign_*` and embedding the URL in a public response without a
  bound `expire`.** The URL is a bearer token.
- **Using `skip_signature` for a private bucket.** No requests will be
  authenticated; whether they are accepted is the bucket policy's call. If
  the bucket is private, every request will 4xx.
- **Mixing `disable_config_load` with code that *also* mutates AWS env vars at
  runtime.** Hermeticity is per `Operator`, not per process.
- **Treating an integration repo (`dav-server`, `unftp-sbe`,
  `object_store`, `parquet`, `spring`) as if it inherited the same trust
  posture as the core.** Each adds a different deployment shape.
- **Embedding OpenDAL in a long-running service and never tuning timeouts.**
  A backend that hangs will hang the caller; OpenDAL has no default cap.

## §11a Known non-findings (recurring false positives)

The following are expected to come up from automated scanners and AI
analysers and are **not** OpenDAL vulnerabilities given the model.

- **"`Operator::read(user_input)` allows path traversal."** Out of model
  per §3 and §9.4. `path` is trusted operator input by §6.
  `OUT-OF-MODEL: trusted-input`.
- **"`Builder::endpoint(user_input)` allows SSRF."** Out of model per §3
  and §9.7. The `Builder` is operator config. `OUT-OF-MODEL: trusted-input`.
- **"`services::Fs` follows symlinks."** Documented non-property per §9.4.
  `BY-DESIGN: property-disclaimed`.
- **"S3 / GCS / Azure client trusts the endpoint response without
  re-validating signatures."** The backend is treated as control-plane
  trusted per §4 boundary 4 / §3. `OUT-OF-MODEL: trusted-input`.
- **"`Content-Length` mismatch is not enforced."** Documented non-property
  per §9.10. `BY-DESIGN: property-disclaimed`.
- **"Unbounded memory growth when reading a large object."** Documented
  non-property per §9.1. The caller is responsible for `TimeoutLayer` /
  caps per §10. `BY-DESIGN: property-disclaimed`.
- **"Decompression bomb via gzipped response."** §9.6.
  `BY-DESIGN: property-disclaimed`.
- **"`ETag` is not cryptographic integrity."** §9.10.
  `BY-DESIGN: property-disclaimed`.
- **"Logs leak the path of every request."** Expected of `LoggingLayer`;
  not a credential. `BY-DESIGN: property-disclaimed`.
- **"`allow_anonymous` exists on the public API."** Deprecated alias for
  `skip_signature`; documented retirement path in upgrade.md v0.57.
  `KNOWN-NON-FINDING`.
- **"Service builds an HTTPS request from a user-supplied endpoint."**
  Endpoint is operator config. `OUT-OF-MODEL: trusted-input`.
- **"FFI `unsafe` in `bindings/c/src/*.rs`."** In-model only if it
  produces a memory-safety break under the documented API contract; bare
  `unsafe` flagging is not a finding.
- **"Code under `examples/`, `core/edge/`, `core/benches/`, `fixtures/`
  has weak input handling."** `OUT-OF-MODEL: unsupported-component`.

## §12 Conditions that would change this model

- A new service backend that introduces a new credential-bearing state
  type (e.g. a service that requires a long-lived OAuth refresh-token
  pool held by OpenDAL).
- A new layer that touches credentials (e.g. a "credential-cache layer"
  that persists tokens across processes).
- A change in the default of any §5a knob (e.g. `skip_signature` becoming
  true by default for any service).
- A new integration that *terminates* a protocol (a new
  `dav-server`-style repo subdirectory) and is folded into the core's
  trust posture.
- Removal of credential redaction from any `Debug` implementation
  (regressing §8.1).
- A vulnerability report that cannot be cleanly routed to one of the §13
  dispositions — that is evidence the model has a gap.
- Promotion of one of the satellite repos (`opendal-reqsign`,
  `opendal-fastrace-jaeger`, `opendal-website`) back into the main repo.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope §7 adversary, reachable per §4 and §6. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 violation, but the API makes a §11 misuse easy enough that the project elects to harden it. Triaged privately; no CVE by default. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires attacker control of a parameter §6 marks trusted (most commonly `path`, `Builder` fields, or `endpoint`). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires an adversary §7 excludes (in-process attacker, side-channel, malicious trusted backend). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `examples/`, `core/edge/`, `core/benches/`, `fixtures/`, or one of the three out-of-scope satellite repos. | §3 |
| `OUT-OF-MODEL: non-default-build` | Manifests only when a §5a knob is set to a non-default, discouraged value. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a property §9 explicitly does not provide (resource bounds, backend authentication, traversal protection, constant-time, …). | §9 |
| `KNOWN-NON-FINDING` | Matches a documented recurring false positive. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed to any of the above. Triggers a §12 revision. | §12 |

## §14 Open questions for the maintainers

Grouped into waves; each question states a proposed answer for the
maintainer to confirm, correct, or strike. Answers will be folded back into
the body and the corresponding *(inferred)* tag(s) promoted.

### Wave 1 — Scope and intended use

**Q14.1** (§2, §3) — Proposed: the caller is trusted to choose the
`Builder` config and to supply the `path`. End-user authn/authz over the
embedding application's users is explicitly not OpenDAL's job. Confirm?

**Q14.2** (§3) — Proposed: storage-provider authorisation (S3 bucket
policy, POSIX mode, HDFS perms, OAuth scope) is enforced by the storage
service, not by OpenDAL. A misconfigured bucket policy that opens public
read is not an OpenDAL vulnerability. Confirm?

**Q14.3** (§3, §7) — Proposed: transport security (TLS cert validation,
HTTPS vs HTTP, CA bundle) is inherited from the underlying HTTP client
(today: `reqwest`) and from the host environment. Downgrading TLS at the
HTTP-client layer is the caller's choice. Confirm?

**Q14.4** (§3, §9.4, §11) — Proposed: `services::Fs` is not a sandbox.
`..` / symlink-escape via the `path` argument is out of model; sanitising
path components before they reach the `Operator` is documented as the
caller's responsibility. Confirm — and is there interest in shipping an
opt-in "confining" layer or `Fs` knob, or is the stance "out of scope,
always"?

**Q14.5** (§3, §9.9) — Proposed: `integrations/dav-server` and
`integrations/unftp-sbe` provide adapter glue only; authentication and
authorization on the resulting listener are the responsibility of the
host process. Confirm — and is the right stance "integrations are
in-model for code-level bugs, out-of-model for deployment-shape bugs",
or stronger?

### Wave 2 — Trust boundaries and isolation

**Q14.6** (§3, §7) — Proposed: side-channel adversaries (timing,
cache-line, co-tenant) are out of model. Confirm?

**Q14.7** (§4, §8.2, §9.5) — Proposed: per-`Operator` credential
isolation is an invariant the project tries to uphold (no cross-`Operator`
signer / token / context leakage), but there is no synthesised isolation
between two `Operator`s pointing at overlapping `root`s or the same
bucket — those are the caller's namespacing problem. Confirm?

**Q14.8** (§5, §8.4) — Proposed: the safe-Rust core is memory-safe under
the documented API contract on supported platforms, with `unsafe` confined
to FFI seams (HDFS-JNI, certain DB drivers, the C ABI, language bindings).
Confirm — and is there a list of services whose Rust code is more than
"thin wrapper plus safe-Rust HTTP" (notably HDFS, FoundationDB, possibly
RocksDB) that we should call out at a different trust level?

**Q14.9** (§5) — Proposed negative-side-effect inventory: no global signal
handlers, no child-process spawn from the core, no global allocator /
locale / FPU mutation, no global panic hook, no silent HTTPS→HTTP fallback,
no on-disk persistence of credentials by the core itself. Confirm any that
are wrong.

### Wave 3 — Build-time and configuration variants

**Q14.10** (§5a) — Proposed: all rows in the §5a table whose maintainer
stance is currently tagged "Supported" *are* part of the supported
production posture. The corresponding production findings against the
default (`skip_signature = false`, `disable_config_load = false`, etc.) are
`VALID`; findings against the *non*-default value are `OUT-OF-MODEL:
non-default-build` only if you intended the non-default value to be
dev-only. Confirm per row.

**Q14.11** (§6, §8.8, §9.1, §11) — Proposed: OpenDAL makes no
categorical resource bound. A backend returning 1 TB of body, or 10⁶
list pages, is forwarded to the caller. Callers wanting bounds apply
`TimeoutLayer` / `ConcurrentLimitLayer` / `ThrottleLayer`. Confirm — and is
there any property that *is* upheld here (e.g. "the layer composition is
zero-allocation", "list pagination is `O(1)` memory per page")?

**Q14.12** (§8.5) — Proposed: applying any subset of bundled layers in
any documented order does not leak credentials to that layer's outward
channel (log line, metric, span). Confirm?

**Q14.13** (§8.7) — Proposed: `skip_signature = true` controls only
signing — it does NOT switch transport to HTTP, weaken endpoint
validation, or change headers other than auth-related ones. Confirm.

### Wave 4 — Properties NOT provided

**Q14.14** (§9.2) — Proposed: OpenDAL does not authenticate backend bytes.
ETag is treated as a backend-defined integrity hint, not a MAC. Confirm.

**Q14.15** (§9.3) — Proposed: no `Eq` impl in the public API is
documented constant-time; do not use OpenDAL for secret comparison.
Confirm.

**Q14.16** (§9.6) — Proposed: there is no decompressed-output cap in
OpenDAL. A gzipped-response decompression bomb is the caller's problem.
Confirm.

**Q14.17** (§9.8) — Proposed: each language binding under `bindings/` has
an independent version. A property that holds for the Rust core at
version *X* is not guaranteed in the Python (or Java, Node, …) binding
at version *X* unless that binding has been re-released. Confirm — and is
there a single source of truth (matrix, README table) where the embedded
core version for each released binding can be checked?

**Q14.18** (§9.11) — Proposed: amplification behaviour of the response
XML / JSON parsers against malformed backend responses is not bounded by
OpenDAL. Confirm — and is `serde-xml-rs` (or whatever the current XML
parser is) configured with any per-parse limits?

**Q14.19** (§9.11) — Proposed: `LoggingLayer` and `MetricsLayer` emit
caller-supplied paths into their sinks; if the sink interprets bytes
structurally (e.g. a metric label, a JSON-encoded log field), the caller
must scrub before connecting the sink. Confirm.

### Wave 5 — Meta

**Q14.20** (§1, §12) — How should this document coexist with
`website/community/security.md`? Proposed: the website page remains the
disclosure-process landing page and links to this threat model as the
canonical "what counts as a vuln" reference. The two documents are kept
in sync at release time.

**Q14.21** (§1, §12) — Where should this document live? Proposed:
`docs/threat-model.md` (or `website/community/threat-model.md`) in
`apache/opendal`, versioned with the release, with the website rendering
it under the Security menu.

**Q14.22** (§12) — Proposed revision trigger: change in default of any
§5a knob, addition of a credential-touching layer, promotion of a
satellite repo back into the main tree, or a vulnerability report that
cannot be routed to one of the §13 dispositions. Confirm.

**Q14.23** (§3) — Proposed: when the threat models for the three
satellite repos (`opendal-reqsign`, `opendal-fastrace-jaeger`,
`opendal-website`) are produced, they each cross-reference this one
rather than restate it. Confirm.

### Edge probes of documented claims

**Q14.24** (§8.1) — The credential-redaction property is documented for
in-tree `Builder`s and the `LoggingLayer`. Are there platforms / build
configs (e.g. `RUST_LOG=trace`, panic-with-backtrace, an alternate
logger backend) where a credential could re-surface via a path the
RFC-0203 redaction does not cover?

**Q14.25** (§8.3) — `normalize_path` strips whitespace and collapses
`//`. Are there path-shape inputs where the current normaliser produces
a backend-side surprise (e.g. paths that differ only in trailing
whitespace ending up as the same key in a backend that treats them
distinctly)?

## §15 Appendix — back-map: existing security-policy artefact → §

The OpenDAL repo's only canonical security-policy artefact today is
`website/community/security.md`. Its content is process-only (where to
report, embargo posture) and therefore maps fully outside this threat
model:

| `website/community/security.md` line | Lands in |
| --- | --- |
| "report to `private@opendal.apache.org`" | §1 reporting cross-reference |
| "do not disclose publicly before reporting" | §1 reporting cross-reference |

There is no embedded threat-model content in `security.md` today to
back-map. This threat model is additive, not a replacement; Q14.20
proposes how the two coexist.
