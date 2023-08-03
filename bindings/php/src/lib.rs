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
use std::str::FromStr;
use ext_php_rs::prelude::*;
use ext_php_rs::types::ZendClassObject;
use ::opendal as od;

#[php_class]
pub struct OpenDAL {
    op: od::Operator,
}

// #[php_impl]
// #[php_class(name = "OpenDAL\\OpenDAL")]
impl OpenDAL {
    pub fn __construct(scheme_str: String, mp: HashMap<String, String>) -> Self {
        let scheme = od::Scheme::from_str(&scheme_str).unwrap();
        let mut op = od::Operator::via_map(scheme, mp).unwrap();

        Self { op }
    }
}

#[php_class(name = "OpenDAL\\OpenDALException")]
#[extends(ce::exception())]
struct OpenDALException {
    message: String,
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
}
