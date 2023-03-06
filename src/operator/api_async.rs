// Copyright 2022 Datafuse Labs
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

use std::ops::RangeBounds;

use futures::AsyncReadExt;
use futures::StreamExt;
use tokio::io::ReadBuf;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Operato async API.
impl Operator {
    /// Check if this operator can work correctly.
    ///
    /// We will send a `list` request to path and return any errors we met.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.check().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check(&self) -> Result<()> {
        let mut ds = self.object("/").list().await?;

        match ds.next().await {
            Some(Err(e)) if e.kind() != ErrorKind::ObjectNotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Read the specified range of object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::range_reader`]
    ///
    /// # Notes
    ///
    /// - The returning content's length may be smaller than the range specified.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// # o.write(vec![0; 4096]).await?;
    /// let bs = o.range_read(1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_read(&self, path: &str, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        let path = normalize_path(path);

        if !validate_path(&path, ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "read path is a directory")
                    .with_operation("range_read")
                    .with_context("service", self.inner().metadata().scheme())
                    .with_context("path", &path),
            );
        }

        let br = BytesRange::from(range);

        let op = OpRead::new().with_range(br);

        let (rp, mut s) = self.inner().read(&path, op).await?;

        let length = rp.into_metadata().content_length() as usize;
        let mut buffer = Vec::with_capacity(length);

        let dst = buffer.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);

        // Safety: the input buffer is created with_capacity(length).
        unsafe { buf.assume_init(length) };

        // TODO: use native read api
        s.read_exact(buf.initialized_mut()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read from storage")
                .with_operation("range_read")
                .with_context("service", self.inner().metadata().scheme().into_static())
                .with_context("path", &path)
                .with_context("range", br.to_string())
                .set_source(err)
        })?;

        // Safety: read_exact makes sure this buffer has been filled.
        unsafe { buffer.set_len(length) }

        Ok(buffer)
    }
}
