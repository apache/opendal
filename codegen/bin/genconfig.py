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

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from codegen import safeexec
from codegen.config import S3

if __name__ == '__main__':
    codegen = Path(__file__).resolve().parent.parent
    env = Environment(
        loader=FileSystemLoader(codegen / 'template'),
    )
    tmpl = env.get_template("config.rs.j2")

    opendal = codegen.parent
    rust = opendal / 'core' / 'src' / 'services' / 'config.rs'
    with rust.open('w') as f:
        assert tmpl.filename is not None
        template = Path(tmpl.filename).relative_to(opendal)
        output = rust.relative_to(opendal)
        print(f'Generating Rust files from templates in {template} to {output}')
        f.write(tmpl.render(configs=[S3]))

    rustfmt = safeexec.lookup('rustfmt')
    safeexec.run(rustfmt, str(rust), verbose=True)
