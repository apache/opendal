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

#[cfg(feature = "retry")]
use std::fmt::Debug;
use std::io::Result;
use std::sync::Arc;

#[cfg(feature = "retry")]
use backon::Backoff;

use crate::Accessor;
use crate::AccessorMetadata;
use crate::Layer;
use crate::Object;

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
            accessor: layer.layer(self.accessor),
        }
    }

    /// Configure backoff for operators
    ///
    /// This function only provided if feature `retry` is enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use backon::ExponentialBackoff;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let accessor = fs::Backend::build().finish().await?;
    /// let op = Operator::new(accessor).with_backoff(ExponentialBackoff::default());
    /// // All operations will be retried if the error is retryable
    /// let _ = op.object("test_file").read();
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "retry")]
    #[must_use]
    pub fn with_backoff(self, backoff: impl Backoff + Send + Sync + Debug + 'static) -> Self {
        self.layer(backoff)
    }

    fn inner(&self) -> Arc<dyn Accessor> {
        self.accessor.clone()
    }

    /// Get metadata of underlying accessor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let accessor = fs::Backend::build().finish().await?;
    /// let op = Operator::new(accessor);
    /// let meta = op.metadata();
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self) -> AccessorMetadata {
        self.accessor.metadata()
    }

    /// Create a new [`Object`][crate::Object] handle to take operations.
    pub fn object(&self, path: &str) -> Object {
        Object::new(self.inner(), path)
    }

    /// Check if this operator can work correctly.
    ///
    /// We will send a real `stat` request to path and return any errors
    /// we met.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let accessor = fs::Backend::build().finish().await?;
    /// let op = Operator::new(accessor);
    /// op.check(".opendal").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check(&self, path: &str) -> Result<()> {
        // The existences of `.opendal` doesn't matters.
        let _ = self.object(path).is_exist().await?;

        Ok(())
    }
}
