- Proposal Name: `rust_extensions`
- Start Date: 2026-08-25
- RFC PR: [apache/opendal#8207](https://github.com/apache/opendal/pull/8207)
- Related issues: [apache/opendal#6748](https://github.com/apache/opendal/issues/6748) and [apache/opendal#4939](https://github.com/apache/opendal/issues/4939)

> **Status:** This RFC proposes the extension architecture and its compatibility
> contract. Build verification and independent packaging remain open for implementation.

# Summary

Let OpenDAL hosts load trusted out-of-tree services and layers at run time.
Extensions target the exact OpenDAL runtime build used by the host and reuse
OpenDAL's Rust builder, service, and layer interfaces. This lets language
bindings offer separately installed extensions and lets third parties distribute
extensions for a binding's runtime.

# Motivation

[RFC-6828](6828_core.md) split the Rust core, services, and layers into separate
crates. Language bindings still choose which crates to include when they build
their native libraries. A Python user who needs one optional service gets all
services selected for the wheel, and a user who needs a service outside that
selection must arrange a custom binding build. A Rust extension should be
usable by bindings that adopt the same extension architecture without rewriting
the service for each language.

# Guide-level explanation

A user installs a binding and a separately packaged extension built for that
binding's runtime. The binding loads the extension before use. If the extension
registers a service, the user constructs an operator for its scheme using the
binding's normal configuration. A layer provided by an extension composes with
an operator regardless of whether its service was built in or loaded separately.

A service author implements [`raw::Service`](crate::raw::Service) and its
[`Builder`](crate::Builder), and provides the service's usual configuration.
A layer author implements [`raw::Layer`](crate::raw::Layer). Either can publish
from a separate repository. If a binding upgrade changes its runtime build,
users need extension artifacts rebuilt for that build. Extension publishers can
ship separate artifacts for each runtime build they support.

# Reference-level explanation

## Runtime and registration

The host and each extension link to one shared Rust runtime library containing
`opendal-core` and shared dependencies used across the boundary. For a
Tokio-based host, both sides use the same linked Tokio instance so extension
code running on the host's Tokio runtime sees its thread-local runtime context.
Services receive the existing [`OperationContext`](crate::OperationContext),
including its composed executor and HTTP transport.

An extension registers a service builder under a scheme using
[`OperatorRegistry::register`](crate::OperatorRegistry::register). The registry's
existing factory path reads the builder configuration and constructs a regular
[`Operator`](crate::Operator) with a [`Servicer`](crate::raw::Servicer), OpenDAL's
`Arc<dyn ServiceDyn>` service handle. Service capabilities and errors follow
ordinary operator behavior. Looking up an unregistered scheme returns
`ErrorKind::Unsupported`.

Layer registration can use a separate name-to-factory registry. In the prototype,
the factory returns `Arc<dyn Layer>` and a small adapter for
[`Operator::layer`](crate::Operator::layer) forwards
[`Layer::apply_service`](crate::raw::Layer::apply_service) and
[`Layer::apply_context`](crate::raw::Layer::apply_context). This preserves normal
service wrapping and context composition, including reapplying the hooks when
the operator's base context changes. The prototype's `LayerRegistry` and
`extern "Rust"` registration entry point illustrate the design. The RFC leaves
the production registration interface to implementation.

## Build compatibility and lifetime

The extension targets the host's exact runtime build, including the OpenDAL
version, compiler, target, shared dependency builds, and relevant feature and
build settings. Matching version strings alone cannot establish Rust type or
calling-convention compatibility. A loader must check compatibility before
opening the native library, which can execute initializers. If the library is
missing or the loader cannot verify its build, the loader returns an error
without registering factories from that library. The library must come from a
trusted source because its code runs with the host process's privileges.

Keep loaded libraries resident until process exit. Registered factories,
readers, writers, pending futures, and buffers can outlive the loading call or
the operator that created them. Keeping the library resident keeps their code
and static data available, including when Rust drops the last reference.

# Drawbacks

An extension publisher must build for each runtime and platform it supports.
Bindings and extension publishers must coordinate availability when the runtime
changes, even when the extension source still compiles unchanged. Replacing a
loaded extension requires restarting the host.

# Rationale and alternatives

A stable cross-version C ABI is deferred. It would have to define ownership,
threading, polling, wakeups, cancellation, and drop for async futures and I/O
handles. It would need to specify how a segmented or shared `Buffer` is borrowed,
retained, and released, including which allocator frees it, and how Rust panics
are contained at the boundary. It would also need versioned representations for
`Error`, `Metadata`, `Capability`, and operation options that preserve their
semantics as the core evolves. Maintaining and testing those representations
and adapters alongside the Rust interfaces would impose a continuing cost on
changes to core operations and types.

A shared Rust build lets extensions use those values directly and preserves
existing Rust ownership and async behavior. Source builds with statically linked
services and layers still suit users who choose their build configuration in
advance. Dynamic loading lets users add a separately distributed extension to
an installed binding without rebuilding the binding.

# Prior art

[apache/opendal#8511](https://github.com/apache/opendal/pull/8511) built a Rust
host and extension together on Linux x86-64. The extension registered the `Fs`
builder and a `TimeoutLayer` factory. The experiment exercised both layer hooks,
context replay, Tokio wakeups, composition with a host-built memory service, and
a reader and buffer that outlived their operator. It establishes that these Rust
interfaces work across the library boundary in that build. Independent artifact
builds and Python or Java binding integration still require validation.

# Unresolved questions

- How should a runtime distribution identify the build it provides and let
  third-party authors target it? Validate by building an extension from the
  published build inputs and rejecting a mismatched artifact before native code
  runs.
- How should bindings package, locate, and load extensions, and expose layer
  factories to callers? Validate separate installation and ordinary operator
  use in a binding before committing to a packaging or discovery interface.

# Future possibilities

Open a `dev@` discussion about cross-version binary compatibility only if a
concrete downstream requirement shows that rebuilding against the host runtime
is insufficient. For example, a downstream would need to demonstrate why it
must load a newer extension into an older deployed binding and cannot ship a
matching build. Any compatibility promise would require separate community
consensus.
