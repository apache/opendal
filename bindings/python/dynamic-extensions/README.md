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

# Python Dynamic Extension POC

This in-tree POC tests the package and loading model proposed in
[`bindings/docs/dynamic-extensions/python.md`](../../docs/dynamic-extensions/python.md).
It does not change the released Python binding.

The workspace builds three independently linked native artifacts:

- `opendal-runtime-poc` owns protocol negotiation, service registration,
  opaque Python operator handles, library leases, and error transport.
- `s3-extension` owns the S3 builder, operator, dependencies, and operations.
- `fs-extension` owns the FS builder, operator, dependencies, and operations.

The Python package roots model three separately installed distributions. The
main package exposes `opendal.Operator`. Importing `opendal.services.s3` or
`opendal.services.fs` resolves that package's native artifact and registers its
logical manifest without loading native service code. The manifest does not
contain an artifact path or target identity, and configuration crosses the
runtime interface as string pairs.

On Linux, run:

```console
./run-python-linux.sh
```

The separately staged packages support this usage:

```python
import opendal.services.fs
import opendal.services.s3
from opendal import Operator

with Operator("s3", bucket="my-bucket", region="us-east-1") as s3:
    print(s3.info)

with Operator("fs", root="/tmp/opendal") as fs:
    fs.write("hello.txt", b"Hello, OpenDAL!")
    print(fs.read("hello.txt"))
```

The runner executes the complete [`python/example.py`](python/example.py) in
addition to the assertions in `python/test_poc.py`.

The script builds each native artifact in a separate Cargo target directory,
checks the final ELF export allowlists, stages the three Python package roots,
and verifies these behaviors:

1. S3 construction fails before importing its service package.
2. Importing the S3 package registers metadata without loading native code.
3. Constructing an S3 operator loads its package-unique bootstrap and validates
   its package, component, entry symbol, protocol, and OpenDAL identities.
4. Importing the FS package registers FS and completes a real write and read.
5. Closing a Python operator invokes the extension-provided destructor before
   releasing its library lease.

## Deliberate Gaps

This POC keeps the interface small enough to answer the Python packaging
question. It exposes only construction, information, read, write, and
destruction. Each extension owns the Tokio runtime used by its operations.
Consequently, it does not yet prove the selected shared-runtime design's most
important property: one OpenDAL and Tokio graph that can compose arbitrary
native layers.

An earlier iteration transferred independently linked `Operator` values into a
runtime-owned Tokio graph. A real FS operation aborted because the FS library's
Tokio thread-local state could not observe the runtime library's Tokio context.
Another iteration used a Rust `dylib`, but separate S3 and FS builds generated
different runtime binaries because downstream monomorphizations changed the
dylib. The production design must solve this linkage problem or accept a wider
operation interface before extracting layers.

The POC also omits wheel building, automatic entry-point discovery, async
Python operations, aliases, typed configuration, and non-Linux targets.
