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

# PyO3 Extension Comparison Prototype

PROTOTYPE: delete or absorb this directory after the design question is
answered.

This experiment asks whether independently built FS and MIME layer packages can
preserve the base package's PyO3 `Operator` interface. It compares two adapters:

- The capsule adapter passes an exact-build `opendal_core::Operator` through
  named `PyCapsule` values.
- The direct adapter reuses the Rust `PyOperator` definition through ordinary
  Cargo dependencies and lets PyO3 perform extraction.

Both adapters compile from the same sources in separate target directories.
The FS package injects a service-local Tokio runtime layer. The MIME package
applies `MimeGuessLayer`, whose effect is visible through
`Operator.content_type("hello.txt")`.

Run both paths:

```console
./run-linux.sh
```

Run the interactive state viewer:

```console
./run-linux.sh --interactive
```

The capsule is an unsafe exact-build experiment, not a stable ABI. Its payload
contains a Rust `Operator`, so compiler, dependency graph, flags, and OpenDAL
source must match even though the capsule name is versioned.

This comparison adapts the capsule delegation and runtime-switch patterns from
the earlier [split Python binding prototype][split-prototype]. It directly
tests the independently linked `#[pyclass]` constraint discussed in
[PyO3 issue #1444][pyo3-1444] against the current OpenDAL core APIs.

[pyo3-1444]: https://github.com/PyO3/pyo3/issues/1444
[split-prototype]: https://github.com/chitralverma/opendal-python-bindings/blob/main/pyo3-opendal/src/layers.rs
