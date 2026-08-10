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

# Ruby Shared Runtime Prototype

PROTOTYPE: delete or absorb this directory after the design question is
answered.

Question: Can a Ruby binding adapter use the exact language-neutral runtime and
FS service artifacts used by the Python dynamic-extension POC?

The Magnus adapter depends on the extension SDK and loader only. It does not
link OpenDAL core, an OpenDAL service, or Tokio. The runner builds and stages
the existing `opendal-runtime-poc` and `fs-extension` artifacts without
recompiling them for Ruby.

Run the experiment on Linux:

```console
./run-ruby-linux.sh
```

The runner verifies protocol negotiation, lazy FS activation, real FS
write/read, runtime-owned handle destruction, rejection of a newer binding
protocol, and final ELF export allowlists. It also stages the same runtime and
FS binaries under the Python adapter and completes a second real FS round trip.
