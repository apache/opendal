- Proposal Name: `s3_provider_presets`
- Start Date: 2026-07-24
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Add provider presets for storage services implemented through the S3-compatible
API. The initial presets are `r2` and `minio`. Each preset has its own URI
scheme, configuration, and builder while reusing the S3 protocol
implementation.

A preset resolves provider-specific inputs into `S3Config`, wraps the S3 base
service with the provider's identity and effective capabilities, and then lets
`Operator::new` install its default layers. Existing `s3` construction and
behavior remain unchanged.

# Motivation

S3-compatible providers share a protocol but not a complete configuration or
capability contract. Users currently have to discover and repeat endpoint,
region, addressing, multipart, and capability settings for every application
and language binding. Missing a setting can make OpenDAL connect to the wrong
endpoint or advertise an operation variant that the selected provider does not
support.

OpenDAL already owns the S3 request implementation, operator registry, and
capability checks. It should also offer small, tested provider definitions for
widely used S3-compatible services.

The term *preset* is distinct from two existing concepts:

- An alias maps another scheme to the same builder without changing semantics.
- An AWS profile selects credentials from shared AWS configuration.

A provider preset changes construction semantics and does not store
credentials.

# Guide-level explanation

Select Cloudflare R2 explicitly with the `r2` scheme:

```rust
let op = Operator::from_uri((
    "r2://data/root",
    [
        ("account_id", "example-account"),
        ("access_key_id", "example-access-key"),
        ("secret_access_key", "example-secret-key"),
    ],
))?;
```

The URI authority remains the bucket and the path remains the OpenDAL root.
`account_id` lets the preset derive the standard R2 endpoint. Credentials are
passed as extra options rather than embedded in the URI so logs and diagnostics
do not expose them.

Select a MinIO deployment with the `minio` scheme:

```rust
let op = Operator::via_iter(
    "minio",
    [
        ("endpoint", "http://127.0.0.1:9000"),
        ("bucket", "data"),
        ("access_key_id", "minioadmin"),
        ("secret_access_key", "minioadmin"),
    ],
)?;
```

MinIO has no universal endpoint, so the preset requires one instead of falling
back to the AWS S3 endpoint.

Provider selection is always explicit. OpenDAL does not infer a preset from an
endpoint because custom domains, gateways, and proxies make that inference
ambiguous. Users that need a provider-specific configuration outside the
preset contract continue to use `s3`.

# Reference-level explanation

## Service registration

The `opendal-service-s3` crate exports `R2`, `R2Config`, `Minio`, and
`MinioConfig` alongside `S3` and `S3Config`. Enabling `services-s3` registers
all three schemes:

```rust,ignore
registry.register::<S3>("s3");
registry.register::<R2>("r2");
registry.register::<Minio>("minio");
```

`Operator::from_uri` and `Operator::via_iter` therefore expose presets to every
binding that uses scheme-based construction. Typed bindings may expose the
matching provider config types without implementing provider logic outside
Rust.

## Configuration contract

Provider configs contain the common S3 fields plus provider-specific inputs.
They resolve into `S3Config` with this precedence:

```text
S3 defaults < preset defaults < explicit user values < provider validation
```

Preset defaults fill missing values only. Provider validation rejects
combinations that would otherwise be ignored or produce an invalid request.
For example, the R2 preset accepts either an explicit endpoint or an account ID
from which it can derive an endpoint. An explicit endpoint cannot be combined
with endpoint-derived fields such as jurisdiction.

The R2 preset owns endpoint derivation, its signing-region defaults, and its
effective S3 capability reductions. The MinIO preset requires an endpoint,
supplies `auto` when `region` is absent, and applies only provider-wide
capability reductions. It does not infer bucket- or deployment-scoped features
such as versioning. Common S3 credential configuration remains available to
both.

Preset-specific fields are consumed before constructing `S3Config`; they never
become unknown S3 options. Validation errors use `ErrorKind::ConfigInvalid` and
include the preset and field in the error context. Debug output must not include
credentials.

## Service construction

A provider builder resolves its config, builds the normal S3 service, and wraps
that service in an internal `S3PresetService`:

```text
provider config
-> resolved S3Config
-> S3 service
-> S3PresetService
-> Operator default layers
```

`S3PresetService` forwards every operation. It changes only:

- `ServiceInfo::scheme`, which reports `r2` or `minio`; and
- `Service::capability`, which reports the provider's effective capability.

The wrapper is part of the base service returned by the provider builder.
`Operator::new` then installs `ErrorContextLayer`, `SimulateLayer`,
`CompleteLayer`, and `CorrectnessCheckLayer` around it. This ordering ensures
completion and correctness checks observe provider capabilities. Applying a
capability override after constructing an `Operator` would only wrap the
already-installed correctness layer and would not change the capability that
the inner checker reads.

A preset may only remove capabilities or lower limits reported by S3. It cannot
advertise a provider extension unless the shared S3 implementation actually
implements that operation.

## Capability maintenance

Each preset keeps its capability transformation in one provider-owned
function. Changes require an authoritative provider contract and a behavior
test that exercises the preset rather than a manually configured `s3`
operator. Provider behavior tests must verify operations whose capability is
changed, not only the reported `Capability` value.

Managed providers can evolve their S3 compatibility after an OpenDAL release.
Updating a preset to match verified provider behavior is a correctness fix.
Applications that require a frozen manual capability contract can continue to
construct `s3` and apply their own configuration.

# Compatibility and migration

This proposal adds schemes and types without changing `s3`. OpenDAL never
reinterprets an existing S3 endpoint as a preset, so existing applications keep
their construction, identity, and capabilities.

An application can migrate a manual provider configuration by changing the
scheme and deleting values supplied by the preset. Configuration for `r2` or
`minio` fails as an unregistered scheme on older OpenDAL versions instead of
silently falling back to S3.

# Drawbacks

Built-in presets make OpenDAL responsible for tracking provider compatibility.
Stale defaults or capability tables can be worse than documentation because
applications rely on them at runtime. Every preset therefore needs an active
owner, authoritative references, and continuous behavior coverage.

Provider schemes and config types also enlarge generated binding APIs. A
self-hosted provider such as MinIO can vary by version and deployment, so its
preset must remain conservative.

# Rationale and alternatives

Registering `r2` or `minio` as aliases of `S3` would improve naming but could
not supply defaults, validation, identity, or capabilities.

Adding a `profile` string to `S3Config` would mix provider selection with AWS
credential profiles and accumulate provider branches inside `S3Builder`.
Dedicated provider builders keep S3 provider-independent and give static and
dynamic construction the same contract.

Detecting providers from endpoints would make behavior depend on hostname
shape and would fail for custom domains and compatible gateways. Explicit
schemes keep provider semantics observable.

Separate provider crates would duplicate the S3 protocol implementation and
create unnecessary feature and release boundaries. Presets belong in
`opendal-service-s3` because they reuse that implementation without adding
dependencies.

A public generic preset trait is not required for the initial providers. The
shared wrapper and resolver can remain internal until multiple implementations
demonstrate a stable extension contract.

# Prior art

RFC-5444 introduced scheme-based `OperatorRegistry` construction and identified
R2 configuration presets as a future extension. RFC-6707 introduced
`CapabilityOverrideLayer` and identified curated S3-compatible profiles as a
future use case. This proposal places the same capability transformation below
the default correctness layer and gives each provider a first-class
configuration contract.

# Future possibilities

Additional managed providers such as Tigris or Wasabi can add presets when
their defaults and capability contract are backed by maintained behavior
tests. A later RFC may expose application-defined presets through custom
registries without adding credential storage to OpenDAL.
