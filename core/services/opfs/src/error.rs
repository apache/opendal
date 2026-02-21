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

use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use web_sys::DomException;

use opendal_core::Error;
use opendal_core::ErrorKind;

pub(crate) fn parse_js_error(value: JsValue) -> Error {
    if let Some(exc) = value.dyn_ref::<DomException>() {
        let kind = match exc.name().as_str() {
            "NotFoundError" => ErrorKind::NotFound,
            "TypeMismatchError" => ErrorKind::NotFound,
            "NotAllowedError" => ErrorKind::PermissionDenied,
            _ => ErrorKind::Unexpected,
        };
        return Error::new(kind, exc.message());
    }

    // FIXME: how to map `TypeError`` from `getDirectoryHandle`:
    // "name specified is not a valid string or contains characters that
    // would interfere with the native file system"
    if let Some(err) = value.dyn_ref::<js_sys::Error>() {
        return Error::new(ErrorKind::Unexpected, String::from(err.message()));
    }

    Error::new(
        ErrorKind::Unexpected,
        value
            .as_string()
            .unwrap_or_else(|| "unknown JS error".to_string()),
    )
}
