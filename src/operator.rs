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

use std::io::Result;
use std::sync::Arc;

use crate::ops::OpList;
use crate::Accessor;
use crate::Layer;
use crate::Object;
use crate::ObjectStreamer;

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

    /// Create a new [`Object`][crate::Object] handle to take operations.
    pub fn object(&self, path: &str) -> Object {
        Object::new(self.inner(), path)
    }

    /// Create a new [`ObjectStreamer`][crate::ObjectStreamer] handle to list objects.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use futures::StreamExt;
    /// use opendal::ObjectMode;
    /// use opendal::ObjectStream;
    /// use opendal::Operator;
    /// use opendal::services::fs;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);
    ///
    ///     op.object("test_dir/test_file").write("Hello, World!").await?;
    ///
    ///     // Start listing a dir.
    ///     let mut obs = op.objects("test_dir").await?;
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
    pub async fn objects(&self, path: &str) -> Result<ObjectStreamer> {
        let op = OpList::new(&Object::normalize_path(path));
        self.inner().list(&op).await
    }
}
