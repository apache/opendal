# Apache OpenDAL — Threat Model (v1 draft)

**Status:** v1 draft, authored by the ASF Security Team for OpenDAL PMC
review. Drafted against `apache/opendal` default branch, 2026-06-01.

**Provenance legend:** *(documented)* — from the project's own docs/README/
source; *(maintainer)* — ratified by an OpenDAL PMC member; *(inferred)* —
reasoned from code structure / domain norms, **not yet confirmed** (every
*(inferred)* claim has a matching item in §14 Open questions).

**Draft confidence:** documented ~24 · maintainer 0 · inferred ~21. This is a
react-to-me draft, not a finished model — the OpenDAL PMC (xuanwo, tison)
confirms/corrects, especially the §14 items.

**Revision triggers:** a new service backend with a novel auth/credential
shape; a new layer that touches credentials, paths, or request routing; a
change to the FFI/binding boundary; a change to default TLS/endpoint handling.

---

## §1 Header

- **Project:** Apache OpenDAL™ (`apache/opendal`) — "One Layer, All Storage":
  a unified data-access layer (`Operator` abstraction) over object storage,
  file systems, cloud SaaS, databases, protocols, and key-value services.
  *(documented — README)*
- **Scope of this model:** the `apache/opendal` repository — the Rust core
  (`core/`), the ~70 service backends (`core/services/`), the ~25 layers
  (`core/layers/`), and the language bindings (`bindings/`). The sibling
  repos `opendal-reqsign`, `opendal-go-services`, and `opendal-oli` get their
  own models (separate Glasswing PRs). *(documented — repo layout)*
- **What OpenDAL is not:** not a storage server, not a sandbox, not an
  auth/identity provider. It is an in-process client library that brokers a
  caller's data access to a configured backend. *(inferred — Q1)*

## §2 Scope and intended use

OpenDAL is embedded **in-process** in a host application (or invoked through
a language binding) to read/write/list data on a backend the host configures.
*(documented — README "in-process library", `Operator`)*

### Component families (distinct threat profiles)

| Family | Path | Role / trust notes |
|---|---|---|
| Core (`Operator`/`Accessor`, types, raw) | `core/src/` | The public API surface + the abstraction every backend implements. *(documented)* |
| Service backends (~70) | `core/services/` | Each speaks a remote/local protocol: cloud object stores (s3, gcs, azblob, azdls, oss, cos, obs, b2, swift), HDFS/WebHDFS, SFTP/FTP/WebDAV, SaaS (gdrive, onedrive, dropbox, …), databases (mysql, postgresql, mongodb, surrealdb, …), KV/cache (redis, memcached, etcd, tikv, rocksdb, …), local FS, and HTTP. Each handles its own **credentials + auth + transport**. *(documented — `core/services/`)* |
| Layers (~25) | `core/layers/` | Cross-cutting middleware: retry, timeout, throttle, concurrent-limit, logging, tracing, metrics, capability-check, immutable-index, etc. Some touch security-relevant behaviour (logging may surface request metadata; retry/concurrent-limit affect resource bounds). *(documented — `core/layers/`)* |
| Language bindings | `bindings/` | C, C++, Go, Python, Java, Node.js, and more — each crosses an FFI/ABI boundary into the Rust core. *(documented — README/`bindings/`)* |

**In scope:** core + service backends + layers + the bindings' glue to the
core. **Intended caller trust:** the host application and its configuration
(endpoints, credentials) are **trusted**; the *data* and *paths* the host
passes through OpenDAL may be attacker-influenced. *(inferred — Q2)*

## §3 Out of scope (explicit non-goals)

- `examples/`, `fuzz/`, `testkit/`, `tests/`, `benches/`, `edge/`, `dev/`,
  `fixtures/` — not shipped as supported product. *(inferred — Q3)*
- The **remote services themselves** (S3, GCS, a Postgres server, …). OpenDAL
  is a *client*; a vulnerability in the backend service is the backend's, not
  OpenDAL's. *(inferred — Q3)*
- **Credential acquisition / storage / rotation.** OpenDAL consumes
  credentials the host supplies via config; how the host obtains, stores, and
  rotates them is the host's responsibility. *(inferred — Q10)*
- **Encryption at rest** and object-level integrity beyond what the backend
  protocol provides. *(inferred — Q9)*
- Host-language memory safety on the far side of a binding's FFI boundary
  (the binding marshals correctly; what the host language does with the bytes
  is the host's). *(inferred — Q3)*

## §4 Trust boundaries and data flow

1. **Caller → `Operator` API.** The in-process boundary. The caller is
   trusted to be non-malicious (an in-process caller already has full process
   control — §7). What crosses is paths, data, and config. *(inferred — Q4)*
2. **OpenDAL → remote backend (network).** The high-value boundary: requests
   carry credentials/signatures; responses are **backend-controlled bytes**.
   TLS (where the backend supports it) protects this hop. *(inferred — Q5,Q6)*
3. **Config/credential ingestion.** Endpoints, regions, keys, tokens enter via
   builder/config. A caller that lets *untrusted* input reach config (e.g. a
   user-supplied endpoint for the `http` service) changes the trust picture
   (SSRF surface). *(inferred — Q7)*
4. **FFI/binding boundary.** Each binding marshals across the Rust↔host ABI.
   *(documented — `bindings/`)*

## §5 Assumptions about the environment

- Rust core; async via the host's runtime (typically Tokio). *(inferred — Q19)*
- For local backends (`fs`, `sftp`, `ftp`, `hdfs`, …) the OS path semantics and
  filesystem permissions are the OS's; OpenDAL passes paths through. *(inferred — Q7)*
- Network reachability to the configured backend; system trust store for TLS.
  *(inferred — Q6)*

## §6 Assumptions about inputs — per-parameter trust

| Input | Source | Trusted? | Notes |
|---|---|---|---|
| Backend config (endpoint, region, bucket) | Host app | Trusted | If host lets untrusted input set the endpoint (esp. `http`), that's an SSRF surface the host owns. *(inferred — Q7)* |
| Credentials (keys, tokens, SAS) | Host app | Trusted (secret) | OpenDAL must not log/leak them (§8/§9, Q8). |
| Object path / key | Caller (may be attacker-influenced) | Untrusted-capable | Path normalization + traversal handling across backends is security-relevant. *(inferred — Q7)* |
| Object data (write payload) | Caller | Pass-through | OpenDAL doesn't interpret payload semantics. *(inferred)* |
| Range / offset / length params | Caller | Untrusted-capable | Bounds handling on reads/multipart. *(inferred — Q11)* |
| Backend HTTP responses (bodies, headers, redirects, listing XML/JSON) | Remote backend | **Untrusted-capable** | A hostile/compromised backend can return adversarial sizes, redirects, malformed listings. Is parsing hardened against this? *(inferred — Q5)* |

## §7 Adversary model

- **Primary (in scope):** an actor who controls or influences the **data and
  paths** the host passes through OpenDAL, and/or who sits on the
  **network path** to the backend (MITM, absent/misconfigured TLS).
  *(inferred — Q8)*
- **Secondary (likely in scope):** a **malicious or compromised backend
  endpoint** returning adversarial responses (oversized bodies, decompression
  pressure, malicious redirects, malformed list output). *(inferred — Q5)*
- **Out of scope:** the **in-process caller** — already has full control of the
  host process; not a meaningful adversary at this layer. *(inferred — Q4)*
  Side-channel / co-tenant adversaries against the host process. *(inferred — Q10)*

## §8 Security properties the project provides *(all pending PMC confirmation — Q8/Q9/Q11)*

- **Credential confidentiality in logs/traces:** the logging/tracing layers
  are expected **not** to emit raw credentials or full signed Authorization
  headers. *(inferred — Q8)* — violation symptom: a key/token/SAS appears in
  log/trace/metric output; severity: high.
- **Transport security to cloud backends:** HTTPS endpoints by default for
  cloud object stores. *(inferred — Q6)* — violation: silent downgrade to
  plaintext for a backend that supports TLS; severity: high.
- **Memory safety** for well-formed inputs (Rust core, `#![forbid(unsafe)]`?
  unknown for FFI). *(inferred — Q12)*
- **Resource bounds are opt-in** via layers (`concurrent-limit`, `throttle`,
  `timeout`, `retry` caps). *(documented — `core/layers/`)* — i.e. OpenDAL
  provides the *mechanism*; the host sets the policy.

## §9 Security properties the project does *not* provide

- **Not a sandbox / not an isolation boundary.** A trusted caller can do
  anything the configured backend + credentials allow. *(inferred — Q1)*
- **No defense against a fully hostile backend** beyond protocol correctness —
  if you point OpenDAL at a malicious endpoint, response bytes are still
  parsed. *(inferred — Q5)*
- **Presigned URLs are bearer capabilities,** not an authentication boundary:
  anyone holding a presigned URL has the access it encodes for its lifetime.
  *(inferred — Q13)*
- **No object integrity/authenticity guarantee** beyond what the backend
  protocol offers (e.g. an ETag/CRC is error-detection, not a MAC against a
  malicious backend). *(inferred — Q9)*
- **No automatic bounding of object/listing sizes** — a `read` of a
  10 GiB object returns 10 GiB unless the caller streams/limits. *(inferred — Q11)*
- **No SSRF protection** if the host wires untrusted input into the endpoint/
  `http`-service config — endpoint choice is the host's. *(inferred — Q7)*

## §10 Downstream (integrator) responsibilities

- Manage, scope, and rotate credentials; never source backend **config**
  (esp. endpoints) from untrusted input without validation. *(inferred — Q7,Q10)*
- Sanitize/validate **paths** derived from untrusted input before passing them
  as object keys (path-traversal on `fs`/`sftp`/`ftp`/`webdav`). *(inferred — Q7)*
- Bound object sizes / streaming / concurrency via the provided layers for
  untrusted or unbounded data. *(inferred — Q11)*
- Treat presigned URLs as secrets with the right TTL. *(inferred — Q13)*
- Ensure TLS endpoints for sensitive data; supply the trust store. *(inferred — Q6)*

## §11 Known misuse patterns

- Wiring an **attacker-controlled endpoint/URL** into the `http` service (or
  any endpoint config) → SSRF to internal resources. *(inferred — Q7)*
- Using a **non-TLS** endpoint for a cloud backend over an untrusted network.
  *(inferred — Q6)*
- Passing **untrusted user input as an object path** without normalization.
  *(inferred — Q7)*
- Treating the backend's checksum/ETag as proof of authenticity against a
  malicious backend. *(inferred — Q9)*

### §11a Known non-findings (recurring false positives) *(seed — PMC to expand, Q18a)*

- "OpenDAL connects to whatever endpoint is configured" — connecting to an
  internal/operator-chosen endpoint is **operator configuration**, not a
  library vuln. *(inferred — Q7)*
- "Credentials are read from env/config" — credential sourcing is the host's;
  presence of an access-key field is not a finding. *(inferred — Q10)*
- "The retry layer can multiply requests" — that is a configured tradeoff, not
  a defect; the host sets retry policy. *(inferred — Q11)*
- "A presigned URL grants access without login" — by design (bearer
  capability). *(inferred — Q13)*

## §12 Conditions that would change this model

New backend with a novel auth/credential model; a layer that newly logs or
routes credentials/paths; FFI/ABI change in a binding; a change to default
endpoint/TLS behaviour. *(inferred)*

## §13 Triage dispositions

| Disposition | When | Licensing section |
|---|---|---|
| **VALID** | Violates a §8 property under the §7 adversary, in in-scope code | §7, §8 |
| **OUT-OF-MODEL** | Adversary is the in-process caller, or code in `examples/`/`tests/`/`fuzz/` | §3, §7 |
| **DOWNSTREAM-RESPONSIBILITY** | Credential mgmt, endpoint choice, path sanitization, size bounding | §10 |
| **DISCLAIMED** | A §9 non-guarantee (presign bearer, hostile-backend bytes, integrity-vs-MAC) | §9 |
| **MODEL-GAP** | Plausible but not covered here → escalate to PMC, may revise | §12 |

## §14 Open questions for the maintainers

Each maps to an *(inferred)* claim above; a one-line confirm/correct is enough.

1. **(Q1)** Is "in-process client library, not a sandbox/server" the right framing?
2. **(Q2)** Confirm: host app + its config/credentials are trusted; data + paths passed through may be attacker-influenced.
3. **(Q3)** Confirm out-of-scope set (examples/fuzz/testkit/tests/benches/edge; remote services themselves; far-side-of-FFI host code).
4. **(Q4)** Is the in-process caller out of the adversary model (as for most libraries)?
5. **(Q5)** Is a **malicious/compromised backend** in scope? How hardened is response/listing parsing against adversarial bytes/redirects/sizes?
6. **(Q6)** TLS posture: HTTPS by default for cloud backends? Any silent plaintext fallbacks? Who supplies the trust store?
7. **(Q7)** SSRF / endpoint trust: is untrusted-endpoint config explicitly the host's responsibility? Path-traversal handling for local backends — normalized by OpenDAL or passed through?
8. **(Q8)** Credential confidentiality: do logging/tracing/metrics layers guarantee no raw credentials/Authorization headers are emitted? At which log levels?
9. **(Q9)** Object integrity: does OpenDAL claim any integrity/authenticity beyond the backend protocol? Confirm ETag/CRC ≠ MAC framing.
10. **(Q10)** Confirm credential acquisition/storage/rotation + co-tenant/side-channel are downstream/out-of-scope.
11. **(Q11)** Resource bounds: confirm size/concurrency bounding is opt-in via layers, and unbounded reads are not a library defect.
12. **(Q12)** Memory-safety posture: is the core `unsafe`-free? What about the FFI/binding layer?
13. **(Q13)** Presign semantics: confirm presigned URLs are bearer capabilities (not an auth boundary), TTL is the caller's choice.
14. **(Q18a)** What do scanners most often report against OpenDAL that you consider a non-finding? (Expand §11a.)
15. **(meta)** Should this model live as `THREAT_MODEL.md` in-repo (this PR), and is the `AGENTS.md → SECURITY.md → THREAT_MODEL.md` chain the discoverability path you want?

## §15 Machine-readable companion

Not generated for v1. Optional; can follow once prose is ratified.
