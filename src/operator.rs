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

use std::sync::Arc;

use crate::Accessor;
use crate::Layer;
use crate::Object;
use crate::ObjectStream;

/// User-facing APIs for object and object streams.
#[derive(Clone)]
pub struct Operator {
    accessor: Arc<dyn Accessor>,
}

impl Operator {
    /// Create a new operator.
    ///
    /// # Example
    ///
    /// Read more backend init examples in [examples](https://github.com/datafuselabs/opendal/tree/main/examples).
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// /// Example for initiating a fs backend.
    /// use anyhow::Result;
    /// use opendal::services::fs;
    /// use opendal::services::fs::Builder;
    /// use opendal::Accessor;
    /// use opendal::Object;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Create fs backend builder.
    ///     let mut builder: Builder = fs::Backend::build();
    ///     // Set the root for fs, all operations will happen under this root.
    ///     //
    ///     // NOTE: the root must be absolute path.
    ///     builder.root("/tmp");
    ///     // Build the `Accessor`.
    ///     let accessor: Arc<dyn Accessor> = builder.finish().await?;
    ///
    ///     // `Accessor` provides the low level APIs, we will use `Operator` normally.
    ///     let op: Operator = Operator::new(accessor);
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _: Object = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new(accessor: Arc<dyn Accessor>) -> Self {
        Self { accessor }
    }

    /// Create a new layer.
    #[must_use]
    pub fn layer(self, layer: impl Layer) -> Self {
        Operator {
            accessor: layer.layer(self.accessor.clone()),
        }
    }

    fn inner(&self) -> Arc<dyn Accessor> {
        self.accessor.clone()
    }

    /// Create a new object handle to take operations.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use futures::AsyncReadExt;
    /// use opendal::services::fs;
    /// use opendal::ObjectMode;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);
    ///
    ///     let o = op.object("test_file");
    ///
    ///     // Write data info file;
    ///     let w = o.writer();
    ///     let n = w
    ///         .write_bytes("Hello, World!".to_string().into_bytes())
    ///         .await?;
    ///     assert_eq!(n, 13);
    ///
    ///     // Read data from file;
    ///     let mut r = o.reader();
    ///     let mut buf = vec![];
    ///     let n = r.read_to_end(&mut buf).await?;
    ///     assert_eq!(n, 13);
    ///     assert_eq!(String::from_utf8_lossy(&buf), "Hello, World!");
    ///
    ///     // Read range from file;
    ///     let mut r = o.range_reader(10, 1);
    ///     let mut buf = vec![];
    ///     let n = r.read_to_end(&mut buf).await?;
    ///     assert_eq!(n, 1);
    ///     assert_eq!(String::from_utf8_lossy(&buf), "l");
    ///
    ///     // Get file's Metadata
    ///     let meta = o.metadata().await?;
    ///     assert_eq!(meta.content_length(), 13);
    ///
    ///     // Delete file.
    ///     o.delete().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn object(&self, path: &str) -> Object {
        Object::new(self.inner(), path)
    }

    /// Create a new object stream handle to list objects.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use futures::StreamExt;
    /// use opendal::ObjectMode;
    /// use opendal::ObjectStream;
    /// use opendal::Operator;
    /// use opendal_test::services::fs;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Using opendal internal test framework for example.
    ///     // Don't use this in production.
    ///     // Please init your backend via related example instead.
    ///     let acc = fs::new().await?;
    ///     if acc.is_none() {
    ///         return Ok(());
    ///     }
    ///     let op = Operator::new(acc.unwrap());
    ///
    ///     // Real example starts from here.
    ///
    ///     // Start listing a dir.
    ///     let mut obs: ObjectStream = op.objects("test_dir");
    ///     // ObjectStream implements `futures::Stream`
    ///     while let Some(o) = obs.next().await {
    ///         let mut o = o?;
    ///         // It's highly possible that OpenDAL already did metadata during list.
    ///         // Use `Object::metadata_cached()` to get cached metadata at first.
    ///         let meta = o.metadata_cached().await?;
    ///         match meta.mode() {
    ///             ObjectMode::FILE => {
    ///                 println!("Handling file")
    ///             }
    ///             ObjectMode::DIR => {
    ///                 println!("Handling dir like start a new list via meta.path()")
    ///             }
    ///             ObjectMode::Unknown => continue,
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn objects(&self, path: &str) -> ObjectStream {
        ObjectStream::new(self.inner(), path)
    }
}
