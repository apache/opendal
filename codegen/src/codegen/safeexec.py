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

import sys
from subprocess import PIPE, Popen
from typing import Optional, Any, Set, IO


def run(
    *args: str,
    msg: Optional[str] = None,
    verbose: bool = False,
    codes: Optional[Set[int]] = None,
    **kwargs: Any
) -> Popen[str]:
    sys.stdout.flush()
    if verbose:
        print(f"$ {' '.join(args)}")

    p = Popen(args, **kwargs)
    code = p.wait()
    codes = codes or {0}
    if code not in codes:
        err = f"\nfailed to run: {args}\nexit with code: {code}\n"
        if msg:
            err += f"error message: {msg}\n"
        raise RuntimeError(err)

    return p


def run_pipe(
    *args: str,
    msg: Optional[str] = None,
    verbose: bool = False,
    codes: Optional[Set[int]] = None,
    **kwargs: Any
) -> IO[str]:
    p = run(*args, msg=msg, verbose=verbose, codes=codes, stdout=PIPE, universal_newlines=True, **kwargs)
    assert p.stdout is not None
    return p.stdout


def lookup(command: str, msg: Optional[str] = None) -> str:
    return run_pipe("which", command, msg=msg).read().strip()
