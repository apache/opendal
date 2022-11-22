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

use futures::io::Cursor;
use time::Duration;

use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpPresign;
use crate::ops::OpWriteMultipart;
use crate::ops::PresignedRequest;
use crate::path::normalize_path;
use crate::Accessor;
use crate::Object;
use crate::Result;

/// ObjectMultipart represent an ongoing multipart upload.
///
/// # Process
///
/// ```txt
/// create
///     -> write
///         -> complete to build a normal Object
///         -> abort to cancel upload and delete all existing parts
/// ```
///
/// # Notes
///
/// Before [`ObjectMultipart::complete`] has been called, we can't read any content from this multipart object.
pub struct ObjectMultipart {
    acc: Arc<dyn Accessor>,
    path: String,
    upload_id: String,
}

impl ObjectMultipart {
    /// Build a new MultipartObject.
    pub fn new(acc: Arc<dyn Accessor>, path: &str, upload_id: &str) -> Self {
        Self {
            acc,
            path: normalize_path(path),
            upload_id: upload_id.to_string(),
        }
    }

    /// Write a new [`ObjectPart`] with specified part number.
    pub async fn write(&self, part_number: usize, bs: impl Into<Vec<u8>>) -> Result<ObjectPart> {
        let bs = bs.into();

        let op = OpWriteMultipart::new(self.upload_id.clone(), part_number, bs.len() as u64);
        let r = Cursor::new(bs);
        let rp = self
            .acc
            .write_multipart(&self.path, op, Box::new(r))
            .await?;
        Ok(rp.into_object_part())
    }

    /// Complete multipart uploads with specified parts.
    ///
    /// # Notes
    ///
    /// - This operation will complete and finish this upload.
    /// - This operation will concat input parts to build a new object.
    /// - Input parts order is **SENSITIVE**, please make sure the order is correct.
    pub async fn complete(&self, parts: Vec<ObjectPart>) -> Result<Object> {
        let op = OpCompleteMultipart::new(self.upload_id.clone(), parts);
        self.acc.complete_multipart(&self.path, op).await?;

        Ok(Object::new(self.acc.clone(), &self.path))
    }

    /// Abort multipart uploads.
    ///
    /// # Notes
    ///
    /// - This operation will cancel this upload.
    /// - This operation will remove all parts that already uploaded.
    /// - This operation will return `succeeded` even when object or upload_id not exist.
    pub async fn abort(&self) -> Result<()> {
        let op = OpAbortMultipart::new(self.upload_id.clone());
        let _ = self.acc.abort_multipart(&self.path, op).await?;

        Ok(())
    }

    /// Presign an operation for write multipart.
    ///
    /// # TODO
    ///
    /// User need to handle the response by self which may differ for different platforms.
    pub fn presign_write(&self, part_number: usize, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(
            OpWriteMultipart::new(self.upload_id.clone(), part_number, 0).into(),
            expire,
        );

        let rp = self.acc.presign(&self.path, op)?;
        Ok(rp.into_presigned_request())
    }
}

/// ObjectPart is generated by `write_multipart` operation, carries
/// required information for `complete_multipart`.
#[derive(Debug, Clone, Default)]
pub struct ObjectPart {
    part_number: usize,
    etag: String,
}

impl ObjectPart {
    /// Create a new part
    pub fn new(part_number: usize, etag: &str) -> Self {
        Self {
            part_number,
            etag: etag.to_string(),
        }
    }

    /// Get part_number from part.
    pub fn part_number(&self) -> usize {
        self.part_number
    }

    /// Get etag from part.
    pub fn etag(&self) -> &str {
        &self.etag
    }
}
