#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Capture native Cargo diagnostics without changing its exit status."""
import datetime
import json
import os
import resource
import subprocess
import sys
import time

started = time.monotonic()
args = sys.argv[1:]
with open(os.environ["OPENDAL_CARGO_TRACE"], "a", buffering=1) as trace:
    def record(message):
        stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        trace.write(f"[{stamp}] {message}\n")

    record("COMMAND " + json.dumps(args))
    # Quiet output is the only command option changed by this experiment.
    command = [os.environ["OPENDAL_REAL_RUSTUP"], *[arg for arg in args if arg != "--quiet"]]
    child = subprocess.Popen(command, stderr=subprocess.PIPE)
    for line in child.stderr:
        record(line.decode(errors="replace").rstrip())
        # The native library closes stderr when it enables quiet mode.
        if sys.stderr is not None:
            sys.stderr.buffer.write(line)
            sys.stderr.buffer.flush()
    code = child.wait()
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    record(f"EXIT {code}; wall={time.monotonic() - started:.3f}s; user={usage.ru_utime:.3f}s; system={usage.ru_stime:.3f}s")
sys.exit(code)
