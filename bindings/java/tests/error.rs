// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use enum_iterator::all;
use jni::objects::{JString, JValue};
use jni::{InitArgsBuilder, JavaVM};
use opendal::ErrorKind;
use std::env;

#[test]
fn test_opendal_error_exhausted() -> anyhow::Result<()> {
    let args = {
        let basedir = env!("CARGO_MANIFEST_DIR");
        let classpath = format!("{basedir}/target/classes");
        InitArgsBuilder::new()
            .option(format!("-Djava.class.path={classpath}"))
            .build()?
    };
    let vm = JavaVM::new(args)?;
    let mut env = vm.attach_current_thread()?;

    for e in all::<ErrorKind>() {
        let name = env.new_string(e.to_string())?;
        let code = env.call_static_method(
            "org/apache/opendal/exception/ODException$Code",
            "valueOf",
            "(Ljava/lang/String;)Lorg/apache/opendal/exception/ODException$Code;",
            &[JValue::Object(&name)],
        )?;
        let code = env.call_method(code.l()?, "name", "()Ljava/lang/String;", &[])?;
        let code = JString::from(code.l()?);
        assert_eq!(e.to_string(), env.get_string(&code)?.to_str()?);
    }
    Ok(())
}
