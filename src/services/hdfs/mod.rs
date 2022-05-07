// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
//!
//! A distributed file system that provides high-throughput access to application data.
//!
//! # Features
//!
//! HDFS support needs to enable feature `services-hdfs`.
//!
//! # Environment
//!
//! HDFS needs some environment set correctly.
//!
//! - `JAVA_HOME`: the path to java home, could be found via `java -XshowSettings:properties -version`
//! - `HADOOP_HOME`: the path to hadoop home, opendal relays on this env to discover hadoop jars and set `CLASSPATH` automatically.
//!
//! Most of the time, setting `JAVA_HOME` and `HADOOP_HOME` is enough. But there are some edge cases:
//!
//! - If meeting errors like the following:
//!
//! ```shell
//! error while loading shared libraries: libjvm.so: cannot open shared object file: No such file or directory
//! ```
//!
//! Java's lib are not including in pkg-config find path, please set `LD_LIBRARY_PATH`:
//!
//! ```shell
//! export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
//! ```
//!
//! The path of `libjvm.so` could be different, please keep an eye on it.
//!
//! - If meeting errors like the following:
//!
//! ```shell
//! (unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
//! ```
//!
//! `CLASSPATH` is not set correctly or your hadoop installation is incorrect.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::hdfs;
//! use opendal::services::hdfs::Builder;
//! use opendal::Accessor;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create fs backend builder.
//!     let mut builder: Builder = hdfs::Backend::build();
//!     // Set the name node for hdfs.
//!     builder.name_node("hdfs://127.0.0.1:9000");
//!     // Set the root for hdfs, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/tmp");
//!     // Build the `Accessor`.
//!     let accessor: Arc<dyn Accessor> = builder.finish().await?;
//!
//!     // `Accessor` provides the low level APIs, we will use `Operator` normally.
//!     let op: Operator = Operator::new(accessor);
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;
pub use backend::Builder;

mod error;
mod object_stream;

#[doc(hidden)]
#[cfg(feature = "testing")]
pub mod tests;
