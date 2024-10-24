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

use opendal_v0_49::raw::{
    AccessorInfo, OpBatch, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename,
    OpStat, OpWrite, RpBatch, RpCopy, RpCreateDir, RpDelete, RpList, RpPresign, RpRead, RpRename,
    RpStat, RpWrite,
};
use opendal_v0_49::Buffer;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

/// Convert an opendal v0.50 `Operator` into an opendal v0.49 `Operator` for compatibility.
///
/// ```rust
/// use opendal_v0_50::Operator;
/// use opendal_v0_50::services::MemoryConfig;
/// use opendal_v0_50::Result;
///
/// fn i_need_opendal_v0_49_op(op: opendal_v0_49::Operator) {
///     // do something with old opendal;
/// }
///
/// fn main() -> Result<()> {
///     let v0_50_op = Operator::from_config(MemoryConfig::default())?.finish();
///     let v0_49_op = opendal_compat::v0_50_to_v0_49(v0_50_op);
///     i_need_opendal_v0_49_op(v0_49_op);
///     Ok(())
/// }
/// ```
pub fn v0_50_to_v0_49(v: opendal_v0_50::Operator) -> opendal_v0_49::Operator {
    let acc = CompatAccessor(v.into_inner());
    opendal_v0_49::OperatorBuilder::new(acc).finish()
}

struct CompatAccessor<A>(A);

impl<A: Debug> Debug for CompatAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<A: opendal_v0_50::raw::Access> opendal_v0_49::raw::Access for CompatAccessor<A> {
    type Reader = CompatWrapper<A::Reader>;
    type Writer = CompatWrapper<A::Writer>;
    type Lister = CompatWrapper<A::Lister>;
    type BlockingReader = CompatWrapper<A::BlockingReader>;
    type BlockingWriter = CompatWrapper<A::BlockingWriter>;
    type BlockingLister = CompatWrapper<A::BlockingLister>;

    fn info(&self) -> Arc<AccessorInfo> {
        let new_info = self.0.info().deref().clone();
        let old_info = convert::raw_oio_accessor_info_into(new_info);

        Arc::new(old_info)
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> opendal_v0_49::Result<RpCreateDir> {
        self.0
            .create_dir(path, opendal_v0_50::raw::OpCreateDir::new())
            .await
            .map_err(convert::error_into)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal_v0_49::Result<RpStat> {
        self.0
            .stat(path, convert::raw_op_stat_from(args))
            .await
            .map(convert::raw_rp_stat_into)
            .map_err(convert::error_into)
    }

    async fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> opendal_v0_49::Result<(RpRead, Self::Reader)> {
        let (rp, reader) = self
            .0
            .read(path, convert::raw_op_read_from(args))
            .await
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_read_into(rp), CompatWrapper(reader)))
    }

    async fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> opendal_v0_49::Result<(RpWrite, Self::Writer)> {
        let (rp, writer) = self
            .0
            .write(path, convert::raw_op_write_from(args))
            .await
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_write_into(rp), CompatWrapper(writer)))
    }

    async fn delete(&self, path: &str, args: OpDelete) -> opendal_v0_49::Result<RpDelete> {
        self.0
            .delete(path, convert::raw_op_delete_from(args))
            .await
            .map(convert::raw_rp_delete_into)
            .map_err(convert::error_into)
    }

    async fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> opendal_v0_49::Result<(RpList, Self::Lister)> {
        let (rp, lister) = self
            .0
            .list(path, convert::raw_op_list_from(args))
            .await
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_list_into(rp), CompatWrapper(lister)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> opendal_v0_49::Result<RpCopy> {
        self.0
            .copy(from, to, convert::raw_op_copy_from(args))
            .await
            .map(convert::raw_rp_copy_into)
            .map_err(convert::error_into)
    }

    async fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal_v0_49::Result<RpRename> {
        self.0
            .rename(from, to, convert::raw_op_rename_from(args))
            .await
            .map(convert::raw_rp_rename_into)
            .map_err(convert::error_into)
    }

    async fn presign(&self, path: &str, args: OpPresign) -> opendal_v0_49::Result<RpPresign> {
        self.0
            .presign(path, convert::raw_op_presign_from(args))
            .await
            .map(convert::raw_rp_presign_into)
            .map_err(convert::error_into)
    }

    async fn batch(&self, args: OpBatch) -> opendal_v0_49::Result<RpBatch> {
        self.0
            .batch(convert::raw_op_batch_from(args))
            .await
            .map(convert::raw_rp_batch_into)
            .map_err(convert::error_into)
    }

    fn blocking_create_dir(
        &self,
        path: &str,
        _: OpCreateDir,
    ) -> opendal_v0_49::Result<RpCreateDir> {
        self.0
            .blocking_create_dir(path, opendal_v0_50::raw::OpCreateDir::new())
            .map_err(convert::error_into)?;
        Ok(RpCreateDir::default())
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> opendal_v0_49::Result<RpStat> {
        self.0
            .blocking_stat(path, convert::raw_op_stat_from(args))
            .map(convert::raw_rp_stat_into)
            .map_err(convert::error_into)
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> opendal_v0_49::Result<(RpRead, Self::BlockingReader)> {
        let (rp, reader) = self
            .0
            .blocking_read(path, convert::raw_op_read_from(args))
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_read_into(rp), CompatWrapper(reader)))
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> opendal_v0_49::Result<(RpWrite, Self::BlockingWriter)> {
        let (rp, writer) = self
            .0
            .blocking_write(path, convert::raw_op_write_from(args))
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_write_into(rp), CompatWrapper(writer)))
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> opendal_v0_49::Result<RpDelete> {
        self.0
            .blocking_delete(path, convert::raw_op_delete_from(args))
            .map(convert::raw_rp_delete_into)
            .map_err(convert::error_into)
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> opendal_v0_49::Result<(RpList, Self::BlockingLister)> {
        let (rp, lister) = self
            .0
            .blocking_list(path, convert::raw_op_list_from(args))
            .map_err(convert::error_into)?;
        Ok((convert::raw_rp_list_into(rp), CompatWrapper(lister)))
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> opendal_v0_49::Result<RpCopy> {
        self.0
            .blocking_copy(from, to, convert::raw_op_copy_from(args))
            .map(convert::raw_rp_copy_into)
            .map_err(convert::error_into)
    }

    fn blocking_rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal_v0_49::Result<RpRename> {
        self.0
            .blocking_rename(from, to, convert::raw_op_rename_from(args))
            .map(convert::raw_rp_rename_into)
            .map_err(convert::error_into)
    }
}

struct CompatWrapper<I>(I);

impl<I: opendal_v0_50::raw::oio::Read> opendal_v0_49::raw::oio::Read for CompatWrapper<I> {
    async fn read(&mut self) -> opendal_v0_49::Result<Buffer> {
        self.0
            .read()
            .await
            .map(convert::buffer_into)
            .map_err(convert::error_into)
    }
}

impl<I: opendal_v0_50::raw::oio::Write> opendal_v0_49::raw::oio::Write for CompatWrapper<I> {
    async fn write(&mut self, buf: Buffer) -> opendal_v0_49::Result<()> {
        self.0
            .write(convert::buffer_from(buf))
            .await
            .map_err(convert::error_into)
    }

    async fn close(&mut self) -> opendal_v0_49::Result<()> {
        self.0.close().await.map_err(convert::error_into)
    }

    async fn abort(&mut self) -> opendal_v0_49::Result<()> {
        self.0.abort().await.map_err(convert::error_into)
    }
}

impl<I: opendal_v0_50::raw::oio::List> opendal_v0_49::raw::oio::List for CompatWrapper<I> {
    async fn next(&mut self) -> opendal_v0_49::Result<Option<opendal_v0_49::raw::oio::Entry>> {
        self.0
            .next()
            .await
            .map(|v| v.map(convert::raw_oio_entry_into))
            .map_err(convert::error_into)
    }
}

impl<I: opendal_v0_50::raw::oio::BlockingRead> opendal_v0_49::raw::oio::BlockingRead
    for CompatWrapper<I>
{
    fn read(&mut self) -> opendal_v0_49::Result<Buffer> {
        self.0
            .read()
            .map(convert::buffer_into)
            .map_err(convert::error_into)
    }
}

impl<I: opendal_v0_50::raw::oio::BlockingWrite> opendal_v0_49::raw::oio::BlockingWrite
    for CompatWrapper<I>
{
    fn write(&mut self, buf: Buffer) -> opendal_v0_49::Result<()> {
        self.0
            .write(convert::buffer_from(buf))
            .map_err(convert::error_into)
    }

    fn close(&mut self) -> opendal_v0_49::Result<()> {
        self.0.close().map_err(convert::error_into)
    }
}

impl<I: opendal_v0_50::raw::oio::BlockingList> opendal_v0_49::raw::oio::BlockingList
    for CompatWrapper<I>
{
    fn next(&mut self) -> opendal_v0_49::Result<Option<opendal_v0_49::raw::oio::Entry>> {
        self.0
            .next()
            .map(|v| v.map(convert::raw_oio_entry_into))
            .map_err(convert::error_into)
    }
}

/// The `convert` module facilitates the conversion between opendal versions v0.50 and v0.49.
///
/// # Notes
///
/// We intentionally implemented all those conversion functions by hand. Since this is a
/// compatibility crate and opendal v0.49 will not receive updates, maintenance concerns are
/// minimal. By handling them manually, we can avoid the overhead associated with maintaining
/// a procedural macro.
///
/// # Safety
///
/// All types converted here are plain data types or depend on other `v1.0` crates such as `bytes`
/// or `anyhow`. This ensures that they are the same types, merely presented under different
/// versions of opendal.
///
/// `transmute` also perform compile time checks to detect any type size mismatch like `OpWrite`
/// in which we added a new field since v0.50.
mod convert {
    use opendal_v0_50::Metakey;
    use std::mem::transmute;

    pub fn error_into(e: opendal_v0_50::Error) -> opendal_v0_49::Error {
        unsafe { transmute(e) }
    }

    pub fn buffer_from(b: opendal_v0_49::Buffer) -> opendal_v0_50::Buffer {
        unsafe { transmute(b) }
    }

    pub fn buffer_into(b: opendal_v0_50::Buffer) -> opendal_v0_49::Buffer {
        unsafe { transmute(b) }
    }

    pub fn raw_oio_accessor_info_into(
        e: opendal_v0_50::raw::AccessorInfo,
    ) -> opendal_v0_49::raw::AccessorInfo {
        let mut info = opendal_v0_49::raw::AccessorInfo::default();
        info.set_name(e.name())
            .set_root(e.root())
            .set_scheme(e.scheme().into_static().parse().unwrap())
            .set_native_capability(capability_into(e.native_capability()));

        info
    }

    /// opendal_v0_50 added a new field `write_with_if_none_match`.
    pub fn capability_into(e: opendal_v0_50::Capability) -> opendal_v0_49::Capability {
        opendal_v0_49::Capability {
            stat: e.stat,
            stat_with_if_match: e.stat_with_if_match,
            stat_with_if_none_match: e.stat_with_if_none_match,
            stat_with_override_cache_control: e.stat_with_override_cache_control,
            stat_with_override_content_disposition: e.stat_with_override_content_disposition,
            stat_with_override_content_type: e.stat_with_override_content_type,
            read: e.read,
            read_with_if_match: e.read_with_if_match,
            read_with_if_none_match: e.read_with_if_none_match,
            read_with_override_cache_control: e.read_with_override_cache_control,
            read_with_override_content_disposition: e.read_with_override_content_disposition,
            read_with_override_content_type: e.read_with_override_content_type,
            write: e.write,
            write_can_multi: e.write_can_multi,
            write_can_empty: e.write_can_empty,
            write_can_append: e.write_can_append,
            write_with_content_type: e.write_with_content_type,
            write_with_content_disposition: e.write_with_content_disposition,
            write_with_cache_control: e.write_with_cache_control,
            write_with_user_metadata: e.write_with_user_metadata,
            write_multi_max_size: e.write_multi_max_size,
            write_multi_min_size: e.write_multi_min_size,
            write_multi_align_size: e.write_multi_align_size,
            write_total_max_size: e.write_total_max_size,
            create_dir: e.create_dir,
            delete: e.delete,
            copy: e.copy,
            rename: e.rename,
            list: e.list,
            list_with_limit: e.list_with_limit,
            list_with_start_after: e.list_with_start_after,
            list_with_recursive: e.list_with_recursive,
            presign: e.presign,
            presign_read: e.presign_read,
            presign_stat: e.presign_stat,
            presign_write: e.presign_write,
            batch: e.batch,
            batch_delete: e.batch_delete,
            batch_max_operations: e.batch_max_operations,
            blocking: e.blocking,
        }
    }

    pub fn raw_oio_entry_into(e: opendal_v0_50::raw::oio::Entry) -> opendal_v0_49::raw::oio::Entry {
        unsafe { transmute(e) }
    }

    pub fn raw_op_stat_from(e: opendal_v0_49::raw::OpStat) -> opendal_v0_50::raw::OpStat {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_stat_into(e: opendal_v0_50::raw::RpStat) -> opendal_v0_49::raw::RpStat {
        unsafe { transmute(e) }
    }

    pub fn raw_op_read_from(e: opendal_v0_49::raw::OpRead) -> opendal_v0_50::raw::OpRead {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_read_into(e: opendal_v0_50::raw::RpRead) -> opendal_v0_49::raw::RpRead {
        unsafe { transmute(e) }
    }

    pub fn raw_op_write_from(e: opendal_v0_49::raw::OpWrite) -> opendal_v0_50::raw::OpWrite {
        let mut op = opendal_v0_50::raw::OpWrite::new()
            .with_append(e.append())
            .with_concurrent(e.concurrent());

        if let Some(v) = e.cache_control() {
            op = op.with_cache_control(v);
        }

        if let Some(v) = e.content_type() {
            op = op.with_content_type(v);
        }

        if let Some(v) = e.content_disposition() {
            op = op.with_content_disposition(v);
        }

        if let Some(v) = e.user_metadata() {
            op = op.with_user_metadata(v.clone());
        }

        // We didn't implement `executor` field for `OpWrite`.
        op
    }

    pub fn raw_rp_write_into(e: opendal_v0_50::raw::RpWrite) -> opendal_v0_49::raw::RpWrite {
        unsafe { transmute(e) }
    }

    pub fn raw_op_delete_from(e: opendal_v0_49::raw::OpDelete) -> opendal_v0_50::raw::OpDelete {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_delete_into(e: opendal_v0_50::raw::RpDelete) -> opendal_v0_49::raw::RpDelete {
        unsafe { transmute(e) }
    }

    /// OpenDAL v0.50's OpList has a new field `version`.
    pub fn raw_op_list_from(e: opendal_v0_49::raw::OpList) -> opendal_v0_50::raw::OpList {
        let mut op = opendal_v0_50::raw::OpList::new();

        if let Some(v) = e.limit() {
            op = op.with_limit(v);
        }

        if let Some(v) = e.start_after() {
            op = op.with_start_after(v);
        }

        if e.recursive() {
            op = op.with_recursive(true);
        }

        // There is no way for us to convert `metakey` without depending on `flagset`,
        // let's just hardcode them.
        op = op.with_metakey(Metakey::Mode | Metakey::LastModified);
        op = op.with_concurrent(e.concurrent());

        op
    }

    pub fn raw_rp_list_into(e: opendal_v0_50::raw::RpList) -> opendal_v0_49::raw::RpList {
        unsafe { transmute(e) }
    }

    pub fn raw_op_copy_from(e: opendal_v0_49::raw::OpCopy) -> opendal_v0_50::raw::OpCopy {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_copy_into(e: opendal_v0_50::raw::RpCopy) -> opendal_v0_49::raw::RpCopy {
        unsafe { transmute(e) }
    }

    pub fn raw_op_rename_from(e: opendal_v0_49::raw::OpRename) -> opendal_v0_50::raw::OpRename {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_rename_into(e: opendal_v0_50::raw::RpRename) -> opendal_v0_49::raw::RpRename {
        unsafe { transmute(e) }
    }

    pub fn raw_op_presign_from(e: opendal_v0_49::raw::OpPresign) -> opendal_v0_50::raw::OpPresign {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_presign_into(e: opendal_v0_50::raw::RpPresign) -> opendal_v0_49::raw::RpPresign {
        unsafe { transmute(e) }
    }

    pub fn raw_op_batch_from(e: opendal_v0_49::raw::OpBatch) -> opendal_v0_50::raw::OpBatch {
        unsafe { transmute(e) }
    }

    pub fn raw_rp_batch_into(e: opendal_v0_50::raw::RpBatch) -> opendal_v0_49::raw::RpBatch {
        unsafe { transmute(e) }
    }
}

#[cfg(test)]
mod tests {
    use opendal_v0_50 as new_o;

    #[tokio::test]
    async fn test_read() {
        let new_op = new_o::Operator::from_config(new_o::services::MemoryConfig::default())
            .unwrap()
            .finish();
        let old_op = super::v0_50_to_v0_49(new_op);

        old_op.write("test", "hello, world!").await.unwrap();
        let bs = old_op.read("test").await.unwrap();
        assert_eq!(String::from_utf8_lossy(&bs.to_vec()), "hello, world!");
    }
}
