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

use opendal as od;
use std::{collections::HashMap, str::FromStr};

#[cxx::bridge(namespace = "opendal::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    extern "Rust" {
        type Operator;

        fn new_operator(scheme: &str, configs: Vec<HashMapValue>) -> Box<Operator>;
        fn read(&self, path: &str) -> Vec<u8>;
        fn write(&self, path: &str, bs: &[u8]);
    }
}

struct Operator(od::BlockingOperator);

fn new_operator(scheme: &str, configs: Vec<ffi::HashMapValue>) -> Box<Operator> {
    let scheme = od::Scheme::from_str(scheme).unwrap();

    let map = configs
        .into_iter()
        .map(|value| (value.key, value.value))
        .collect::<HashMap<_, _>>();

    Box::new(Operator(
        od::Operator::via_map(scheme, map).unwrap().blocking(),
    ))
}

impl Operator {
    fn read(&self, path: &str) -> Vec<u8> {
        self.0.read(path).unwrap()
    }

    fn write(&self, path: &str, bs: &[u8]) {
        self.0.write(path, bs.to_owned()).unwrap()
    }
}
