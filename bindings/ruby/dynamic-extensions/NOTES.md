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

Question: Can Ruby use the same shared runtime and native extension artifacts
as Python without linking a binding-private OpenDAL or Tokio graph?

Observed result:

- The Ruby adapter negotiates runtime protocol 1 and rejects a binding that
  requires protocol 2.
- Registering the FS Ruby package does not load its native library. Constructing
  the first FS operator activates it.
- Ruby writes and reads an 8 KiB payload through the runtime-owned handle. The
  adapter exercises the output-buffer resize contract and rejects use after
  `Operator#close`.
- The Ruby adapter links Magnus, the protocol SDK, and the native loader. It
  does not link `opendal-core`, a service crate, or Tokio.
- The same physical runtime and FS artifacts complete a second FS round trip
  through the Python adapter.
- The runtime, FS extension, and Ruby adapter match their explicit ELF export
  allowlists. Ruby requires both `Init_opendal_ruby_poc` and
  `ruby_abi_version`.

Production gate: the current runtime protocol owns service registration and an
operation callback table, but it has no layer registration, `LayerHandle`, or
operator-layer composition function. A successful FS run proves cross-language
runtime reuse only. It does not satisfy the design requirement that arbitrary
native layers preserve `apply_service` and `apply_context` semantics.

Decision: do not migrate the production Python and Ruby bindings yet. The
current runtime implements the Design B operation-table boundary for services,
not the selected Design C shared OpenDAL graph. Production work first needs a
runtime-owned service/layer factory interface and a real operation that proves
an independently packaged native layer composes through that graph.
