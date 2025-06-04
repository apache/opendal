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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::LazyLock;

use ::opendal as od;

mod operator;
mod seek_from;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn new_operator(
    scheme_str: String,
    map: BTreeMap<String, String>,
) -> Result<od::Operator, od::Error> {
    let hm: HashMap<String, String> = map.into_iter().collect();
    let scheme: od::Scheme = od::Scheme::from_str(&scheme_str)?;
    od::Operator::via_iter(scheme, hm)
}

pub fn map_res_error<T>(res: Result<T, od::Error>) -> Result<T, String> {
    res.map_err(|e| e.to_string())
}
