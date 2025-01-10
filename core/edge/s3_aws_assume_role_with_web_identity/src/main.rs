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

use opendal::raw::tests::init_test_service;
use opendal::Result;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    let op = init_test_service()?.expect("service must be init");
    assert_eq!(op.info().scheme(), Scheme::S3);

    let result = op
        .exists(&uuid::Uuid::new_v4().to_string())
        .await
        .expect("this operation should never return error");
    assert!(!result, "the file must be not exist");

    Ok(())
}
