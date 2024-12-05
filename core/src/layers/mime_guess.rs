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

use crate::raw::*;
use crate::Result;

/// A layer that can automatically set `Content-Type` based on the file extension in the path.
///
/// # MimeGuess
///
/// This layer uses [mime_guess](https://crates.io/crates/mime_guess) to automatically
/// set `Content-Type` based on the file extension in the operation path.
///
/// However, please note that this layer will not overwrite the `content_type` you manually set,
/// nor will it overwrite the `content_type` provided by backend services.
///
/// A simple example is that for object storage backends, when you call `stat`, the backend will
/// provide `content_type` information, and `mime_guess` will not be called, but will use
/// the `content_type` provided by the backend.
///
/// But if you use the [Fs](../services/struct.Fs.html) backend to call `stat`, the backend will
/// not provide `content_type` information, and our `mime_guess` will be called to provide you with
/// appropriate `content_type` information.
///
/// Another thing to note is that using this layer does not necessarily mean that the result will 100%
/// contain `content_type` information. If the extension of your path is custom or an uncommon type,
/// the returned result will still not contain `content_type` information (the specific condition here is
/// when [mime_guess::from_path::first_raw](https://docs.rs/mime_guess/latest/mime_guess/struct.MimeGuess.html#method.first_raw)
/// returns `None`).
///
/// # Examples
///
/// ```no_run
/// # use opendal::layers::MimeGuessLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(MimeGuessLayer::default())
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MimeGuessLayer {}

impl<A: Access> Layer<A> for MimeGuessLayer {
    type LayeredAccess = MimeGuessAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        MimeGuessAccessor(inner)
    }
}

#[derive(Clone, Debug)]
pub struct MimeGuessAccessor<A: Access>(A);

fn mime_from_path(path: &str) -> Option<&str> {
    mime_guess::from_path(path).first_raw()
}

fn opwrite_with_mime(path: &str, op: OpWrite) -> OpWrite {
    if op.content_type().is_some() {
        return op;
    }

    if let Some(mime) = mime_from_path(path) {
        return op.with_content_type(mime);
    }

    op
}

fn rpstat_with_mime(path: &str, rp: RpStat) -> RpStat {
    rp.map_metadata(|metadata| {
        if metadata.content_type().is_some() {
            return metadata;
        }

        if let Some(mime) = mime_from_path(path) {
            return metadata.with_content_type(mime.into());
        }

        metadata
    })
}

impl<A: Access> LayeredAccess for MimeGuessAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;
    type BlockingReader = A::BlockingReader;
    type BlockingWriter = A::BlockingWriter;
    type BlockingLister = A::BlockingLister;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.0
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner().read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner()
            .write(path, opwrite_with_mime(path, args))
            .await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner()
            .stat(path, args)
            .await
            .map(|rp| rpstat_with_mime(path, rp))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner().delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner().list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner().blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner()
            .blocking_write(path, opwrite_with_mime(path, args))
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner()
            .blocking_stat(path, args)
            .map(|rp| rpstat_with_mime(path, rp))
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner().blocking_delete()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        self.inner().blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::Memory;
    use crate::Metadata;
    use crate::Operator;
    use futures::TryStreamExt;

    const DATA: &str = "<html>test</html>";
    const CUSTOM: &str = "text/custom";
    const HTML: &str = "text/html";

    #[tokio::test]
    async fn test_async() {
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(MimeGuessLayer::default())
            .finish();

        op.write("test0.html", DATA).await.unwrap();
        assert_eq!(
            op.stat("test0.html").await.unwrap().content_type(),
            Some(HTML)
        );

        op.write("test1.asdfghjkl", DATA).await.unwrap();
        assert_eq!(
            op.stat("test1.asdfghjkl").await.unwrap().content_type(),
            None
        );

        op.write_with("test2.html", DATA)
            .content_type(CUSTOM)
            .await
            .unwrap();
        assert_eq!(
            op.stat("test2.html").await.unwrap().content_type(),
            Some(CUSTOM)
        );

        let entries: Vec<Metadata> = op
            .lister_with("")
            .await
            .unwrap()
            .and_then(|entry| {
                let op = op.clone();
                async move { op.stat(entry.path()).await }
            })
            .try_collect()
            .await
            .unwrap();
        assert_eq!(entries[0].content_type(), Some(HTML));
        assert_eq!(entries[1].content_type(), None);
        assert_eq!(entries[2].content_type(), Some(CUSTOM));
    }

    #[test]
    fn test_blocking() {
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(MimeGuessLayer::default())
            .finish()
            .blocking();

        op.write("test0.html", DATA).unwrap();
        assert_eq!(op.stat("test0.html").unwrap().content_type(), Some(HTML));

        op.write("test1.asdfghjkl", DATA).unwrap();
        assert_eq!(op.stat("test1.asdfghjkl").unwrap().content_type(), None);

        op.write_with("test2.html", DATA)
            .content_type(CUSTOM)
            .call()
            .unwrap();
        assert_eq!(op.stat("test2.html").unwrap().content_type(), Some(CUSTOM));

        let entries: Vec<Metadata> = op
            .lister_with("")
            .call()
            .unwrap()
            .map(|entry| {
                let op = op.clone();
                op.stat(entry.unwrap().path()).unwrap()
            })
            .collect();
        assert_eq!(entries[0].content_type(), Some(HTML));
        assert_eq!(entries[1].content_type(), None);
        assert_eq!(entries[2].content_type(), Some(CUSTOM));
    }
}
