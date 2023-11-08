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

use opendal::services::S3;
use opendal::Operator;
use opendal::Result;
use opendal::Scheme;

fn main() -> Result<()> {
    let op = init_operator_via_builder()?;
    println!("operator from builder: {:?}", op);

    let op = init_operator_via_map()?;
    println!("operator from map: {:?}", op);

    Ok(())
}

fn init_operator_via_builder() -> Result<Operator> {
    let mut builder = S3::default();
    builder.bucket("example");
    builder.region("us-east-1");
    builder.access_key_id("access_key_id");
    builder.secret_access_key("secret_access_key");

    let op = Operator::new(builder)?.finish();
    Ok(op)
}

fn init_operator_via_map() -> Result<Operator> {
    let mut map = HashMap::default();
    map.insert("bucket".to_string(), "example".to_string());
    map.insert("region".to_string(), "us-east-1".to_string());
    map.insert("access_key_id".to_string(), "access_key_id".to_string());
    map.insert(
        "secret_access_key".to_string(),
        "secret_access_key".to_string(),
    );

    let op = Operator::via_map(Scheme::S3, map)?;
    Ok(op)
}
