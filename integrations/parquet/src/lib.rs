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

//! parquet_opendal provides parquet IO utils.
//!
//! ```no_run
//! use parquet::arrow::async_writer::AsyncFileWriter;
//! use parquet::OpendalAsyncWriter;
//! use opendal::services::S3;
//! use opendal::{Builder, Operator};
//!
//! #[tokio::main]
//! async fn main() {
//!     let builder = S3::from_map(
//!         vec![
//!             ("access_key".to_string(), "my_access_key".to_string()),
//!             ("secret_key".to_string(), "my_secret_key".to_string()),
//!             ("endpoint".to_string(), "my_endpoint".to_string()),
//!             ("region".to_string(), "my_region".to_string()),
//!         ]
//!         .into_iter()
//!         .collect(),
//!     ).unwrap();
//!
//!     // Create a new operator
//!     let operator = Operator::new(builder).unwrap().finish();
//!     let path = "/path/to/file.parquet";
//!     // Create a new object store
//!     let mut writer = Arc::new(OpendalAsyncWriter::new(operator.writer(path)));
//! }
//! ```

mod async_writer;

pub use async_writer::OpendalAsyncWriter;
