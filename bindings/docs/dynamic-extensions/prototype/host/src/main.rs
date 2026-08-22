// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::ffi::c_void;
use std::process::ExitCode;

use libloading::{Library, Symbol};
use opendal_core::Operator;

type CreateOperator = unsafe extern "C" fn() -> *mut c_void;
type ApplyLayer = unsafe extern "C" fn(*mut c_void) -> *mut c_void;

fn main() -> ExitCode {
    match unsafe { run() } {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

unsafe fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let s3_path = args.next().ok_or("missing S3 extension path")?;
    let timeout_path = args.next().ok_or("missing Timeout extension path")?;
    if args.next().is_some() {
        return Err("expected exactly two extension paths".to_string());
    }

    let s3 = unsafe { Library::new(&s3_path) }.map_err(|err| err.to_string())?;
    let create: Symbol<'_, CreateOperator> = unsafe {
        s3.get(b"opendal_service_s3_bootstrap_v1\0")
            .map_err(|err| err.to_string())?
    };
    let operator = unsafe { create() };
    if operator.is_null() {
        return Err("S3 extension failed to create an operator".to_string());
    }

    let timeout = unsafe { Library::new(&timeout_path) }.map_err(|err| err.to_string())?;
    let apply: Symbol<'_, ApplyLayer> = unsafe {
        timeout
            .get(b"opendal_layer_timeout_bootstrap_v1\0")
            .map_err(|err| err.to_string())?
    };
    let operator = unsafe { apply(operator) };
    if operator.is_null() {
        return Err("Timeout extension failed to apply its layer".to_string());
    }

    let operator = unsafe { Box::from_raw(operator.cast::<Operator>()) };
    let info = operator.info();
    println!(
        "scheme={} name={} root={}",
        info.scheme(),
        info.name(),
        info.root()
    );

    // Drop the operator before either library so its service and layer vtables
    // still point to loaded code.
    drop(operator);
    drop(timeout);
    drop(s3);
    Ok(())
}
