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

# Prototype Notes

Question: Can separately built FS and MIME layer packages return and transform
the base package's PyO3 `Operator` with and without capsules?

Observed result:

- The capsule FS factory returns the exact base-package Python `Operator` type.
- FS write, read, and `stat` succeed after the operator crosses into the base
  package, proving that the service-local runtime layer and executor enter the
  FS package's Tokio context for these operations.
- The capsule MIME package accepts that operator and changes `hello.txt` from no
  content type to `text/plain`.
- The direct FS package returns an object displayed as
  `opendal_poc.Operator`, but its Python type object differs from the base
  package's type object.
- The direct MIME package rejects both the FS package's operator and the base
  package's operator with `Operator object is not an instance of Operator`.

Conclusion: ordinary Rust dependency reuse does not share a PyO3 class between
independently linked extensions. A capsule can preserve the base Python class
interface, but this prototype's capsule payload is still a Rust `Operator` and
therefore requires an exact build. A production capsule should carry a
versioned function table or another explicitly validated ownership contract
instead of treating the Rust layout as stable.

Known limit: the runtime-switch layer covers service methods, readers, writers,
and the executor used by this experiment. A production implementation must
also wrap returned listers, deleters, and copiers.
