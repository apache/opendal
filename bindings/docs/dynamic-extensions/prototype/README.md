<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Exact-Release Dynamic Loading Prototype

This prototype records one experiment behind the dynamic extension design. It
builds three final artifacts:

- A host executable with its own statically linked `opendal-core`.
- An S3 `cdylib` with its own `opendal-core` and service dependencies.
- A Timeout `cdylib` with its own `opendal-core` and Tokio dependency.

The runner builds each artifact in a separate Cargo invocation and target
directory, so Cargo does not unify their `opendal-core` feature graphs. The host
then loads both extensions with local visibility. The S3 extension creates an
`Operator`, the Timeout extension applies the real `TimeoutLayer`, and the host
reads the resulting operator information. All three artifacts compile against
the same source checkout, lockfile, compiler, target, profile, and Rust flags.

On Linux, run:

```console
./run-linux.sh
```

The script also audits the final ELF dynamic symbol tables. Each extension must
export only its package-unique bootstrap symbol. The local Cargo configuration
passes `--exclude-libs,ALL` to prevent symbols from statically linked dependency
archives from becoming dynamic exports. Explicit version scripts document the
intended allowlists, while the audit remains authoritative because Rust's
`cdylib` link can add exports after a user-supplied version script.

## What It Proves

The prototype shows that an exact-release internal adapter can move one current
`Operator` through separately linked S3 and Timeout artifacts on the tested
Linux toolchain. It also provides concrete artifacts for developing the [symbol
isolation contract](../symbol-isolation.md). It does not implement or conform
to the proposed SDK interface.

## What It Does Not Prove

This is design evidence, not a supported ABI or production loader:

- The prototype exchanges a Rust value through an opaque pointer. The Rust ABI
  remains unstable, so any change in compiler, target, profile, flags, features,
  or dependency graph invalidates the experiment.
- The host ultimately destroys an allocation created and transformed by other
  artifacts. This intentionally violates the proposed ownership contract. The
  production SDK must use opaque handles and creator-side destructors rather
  than treating this experiment as an ownership model.
- The prototype applies `TimeoutLayer` but does not execute an operation.
  Therefore it does not prove that independently linked Tokio copies observe
  compatible runtime state.
- The prototype does not provide the proposed shared runtime package, runtime
  protocol negotiation, manifest validation, error contract, or library lease
  implementation.
- The prototype covers ELF/Linux only. The design still requires equivalent
  Mach-O and PE export enforcement and co-loading tests.
- The bootstrap functions return null on failure and omit structured errors.
  The production contract must use the validated bootstrap envelope.

The prototype should evolve into a conformance fixture only after the RFC
selects the exact-release interface representation.
