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

#![cfg(all(feature = "services-gcs", feature = "services-gcs-s3"))]

use opendal::services::{GcsS3, GcsS3Config};
use opendal::{Operator, OperatorRegistry, Result};

// This integration test has its own process because it replaces a global factory.
#[test]
fn native_gcs_and_s3_preset_coexist_and_allow_explicit_override() -> Result<()> {
    opendal::init_default_registry();

    let native = Operator::from_uri("gcs://example-bucket/data")?;
    assert_eq!(native.info().scheme(), "gcs");

    let from_uri = Operator::from_uri("gcs-s3://example-bucket/data")?;
    let from_options = Operator::via_iter(
        "gcs-s3",
        [
            ("bucket".to_owned(), "example-bucket".to_owned()),
            ("root".to_owned(), "data".to_owned()),
        ],
    )?;
    let mut config = GcsS3Config::default();
    config.bucket = "example-bucket".to_owned();
    config.root = Some("data".to_owned());
    let from_config = Operator::from_config(config)?;
    let from_builder = Operator::new(GcsS3::default().bucket("example-bucket").root("data"))?;

    OperatorRegistry::get().register::<GcsS3>("gcs");
    let remapped = Operator::from_uri("gcs://example-bucket/data")?;
    assert_eq!(native.info().scheme(), "gcs");

    for op in [from_uri, from_options, from_config, from_builder, remapped] {
        assert_eq!(op.info().scheme(), "gcs-s3");
        assert_eq!(op.info().name(), "example-bucket");
        assert_eq!(op.info().root(), "/data/");
    }
    Ok(())
}
