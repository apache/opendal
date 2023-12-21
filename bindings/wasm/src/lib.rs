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

use std::collections::HashMap;
use gloo::utils::format::JsValueSerdeExt;
use opendal::Scheme::S3;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Operator(opendal::Operator);

#[wasm_bindgen]
impl Operator {
    #[wasm_bindgen(constructor)]
    pub fn new(_scheme: String, options: JsValue) -> Result<Operator, JsError> {
        let op = opendal::Operator::via_map(S3, options.into_serde::<HashMap<String,String>>().unwrap()).map_err(format_js_error)?;

        Ok(Operator(op))
    }

    pub async fn is_exist(&self, path: String) -> Result<bool, JsError> {
        self.0.is_exist(&path).await.map_err(format_js_error)
    }

}

fn format_js_error(err: opendal::Error) -> JsError {
    JsError::from(err)
}
