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

use std::sync::Arc;

use bytes::Buf;
use futures::TryStreamExt;

use crate::raw::oio::Write;
use crate::raw::*;
use crate::*;

/// WriteContext holds the immutable context for give write operation.
pub struct WriteContext {
    /// The composed context used to execute operations.
    ctx: OperationContext,
    /// The composed service to the underlying object storage.
    srv: Servicer,
    /// Path to the file.
    path: String,
    /// Arguments for the write operation.
    args: OpWrite,
    /// Options for the writer.
    options: OpWriter,
}

impl WriteContext {
    /// Create a new WriteContext.
    #[inline]
    pub fn new(
        ctx: OperationContext,
        srv: Servicer,
        path: String,
        args: OpWrite,
        options: OpWriter,
    ) -> Self {
        Self {
            ctx,
            srv,
            path,
            args,
            options,
        }
    }

    /// Get the path.
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the composed operation context.
    #[inline]
    pub(crate) fn context(&self) -> &OperationContext {
        &self.ctx
    }

    /// Get the composed service.
    #[inline]
    pub(crate) fn service(&self) -> &Servicer {
        &self.srv
    }

    /// Get the arguments.
    #[inline]
    pub fn args(&self) -> &OpWrite {
        &self.args
    }

    /// Get the options.
    #[inline]
    pub fn options(&self) -> &OpWriter {
        &self.options
    }

    /// Calculate the chunk size for this write process.
    ///
    /// Returns the chunk size and if the chunk size is exact.
    fn calculate_chunk_size(&self) -> (Option<usize>, bool) {
        let cap = self.srv.capability();
        let min_size = cap.write_multi_min_size.filter(|&v| v > 0);
        let max_size = cap.write_multi_max_size.filter(|&v| v > 0);
        let option_chunk = self.options().chunk().filter(|&v| v > 0);

        let exact = option_chunk.is_some();
        let chunk_size = option_chunk.or(min_size).map(|mut size| {
            if let Some(v) = max_size {
                size = size.min(v);
            }
            if let Some(v) = min_size {
                size = size.max(v);
            }

            size
        });

        (chunk_size, exact)
    }
}

/// A source copy that has already appended part of its range.
///
/// The range, path, and read conditions identify the logical call so that a
/// retry can resume after the accepted prefix instead of copying it again.
struct PendingCopy {
    path: String,
    args: OpRead,
    options: OpReader,
    range: BytesRange,
    limit: Option<u64>,
    start: u64,
    end: u64,
    /// The next source offset to copy. It advances only after the destination
    /// accepts the corresponding bytes.
    offset: u64,
}

impl PendingCopy {
    fn has_accepted_bytes(&self) -> bool {
        self.offset > self.start
    }

    fn matches(&self, path: &str, args: &OpRead, range: BytesRange, limit: Option<u64>) -> bool {
        self.path == path && self.range == range && self.limit == limit && self.args.same_read(args)
    }
}

enum WriteLifecycle {
    /// The writer is active and can accept writes, copies, close, or abort.
    Active,
    /// A close has been initiated and can be retried or aborted.
    Closing,
    /// An abort has been requested and underlying cleanup is pending retry.
    Aborting,
    /// The writer has closed successfully.
    Closed,
    /// The writer has been aborted and cleaned up.
    Aborted,
}

/// WriteState owns the mutable state of one logical write call.
///
/// It holds the raw [`oio::Writer`] opened on the composed service together with
/// the chunk buffer, the partially accepted write tracking, and the `copy_from`
/// continuation state. Every writer front-end hands its calls to this state so
/// that chunking and abort rules stay identical across them.
pub struct WriteState {
    w: oio::Writer,

    ctx: Option<Arc<WriteContext>>,

    /// The size for buffer, we will flush the underlying storage at the size of this buffer.
    chunk_size: Option<usize>,
    /// If `exact` is true, the size of the data written to the underlying storage is
    /// exactly `chunk_size` bytes.
    exact: bool,
    /// Whether the composed writer can copy source ranges directly.
    can_copy_from: bool,
    /// Explicit terminal/in-progress lifecycle state of this write call.
    lifecycle: WriteLifecycle,
    /// Whether a logical write stopped after the inner writer accepted part of its input.
    requires_abort: bool,
    /// A source copy that stopped after accepting part of its range.
    pending_copy: Option<Box<PendingCopy>>,
    buffer: oio::QueueBuf,
}

impl WriteState {
    /// Create the write state for the given write context.
    pub fn create(ctx: Arc<WriteContext>) -> Result<Self> {
        let (chunk_size, exact) = ctx.calculate_chunk_size();
        let can_copy_from = ctx.service().capability().write_can_copy_from && !ctx.args().append();
        let w = ctx.srv.write(&ctx.ctx, ctx.path(), ctx.args().clone())?;

        Ok(Self {
            w,
            ctx: Some(ctx),
            chunk_size,
            exact,
            can_copy_from,
            lifecycle: WriteLifecycle::Active,
            requires_abort: false,
            pending_copy: None,
            buffer: oio::QueueBuf::new(),
        })
    }

    /// Allow building from existing oio::Writer for easier testing.
    #[cfg(test)]
    pub(crate) fn new(w: oio::Writer, chunk_size: Option<usize>, exact: bool) -> Self {
        Self {
            w,
            ctx: None,
            chunk_size,
            exact,
            can_copy_from: false,
            lifecycle: WriteLifecycle::Active,
            requires_abort: false,
            pending_copy: None,
            buffer: oio::QueueBuf::new(),
        }
    }

    /// Write as much of the buffer as the next chunk can accept.
    pub async fn write(&mut self, bs: Buffer) -> Result<usize> {
        self.check_writable()?;
        self.write_chunk(bs).await
    }

    pub(crate) async fn write_all(&mut self, mut bs: Buffer) -> Result<()> {
        self.check_writable()?;
        while !bs.is_empty() {
            let n = self
                .write_chunk(bs.clone())
                .await
                .map_err(|err| err.with_persistent(self.requires_abort))?;
            bs.advance(n);
            // A partial write means the inner writer already accepted a chunk of
            // this input. Keep the writer abortable if a later chunk fails.
            self.requires_abort = !bs.is_empty();
        }
        Ok(())
    }

    fn check_writable(&mut self) -> Result<()> {
        self.check_active()?;
        self.check_no_pending_copy()
    }

    fn check_closable(&mut self) -> Result<()> {
        match self.lifecycle {
            WriteLifecycle::Active | WriteLifecycle::Closing => {}
            WriteLifecycle::Closed => {
                return Err(Error::new(ErrorKind::Unexpected, "writer has been closed"));
            }
            WriteLifecycle::Aborting | WriteLifecycle::Aborted => {
                return Err(Error::new(ErrorKind::Unexpected, "writer has been aborted"));
            }
        }

        if self.requires_abort {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "writer must be aborted after a partially accepted write",
            ));
        }

        self.check_no_pending_copy()
    }

    fn check_no_pending_copy(&mut self) -> Result<()> {
        self.pending_copy.take_if(|copy| !copy.has_accepted_bytes());
        if self.pending_copy.is_some() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "writer must be aborted or resume the same copy_from call after a partially accepted copy",
            )
            .set_persistent());
        }
        Ok(())
    }

    fn check_active(&self) -> Result<()> {
        match self.lifecycle {
            WriteLifecycle::Active => {}
            WriteLifecycle::Closing => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer is closing and cannot accept more writes",
                ));
            }
            WriteLifecycle::Closed => {
                return Err(Error::new(ErrorKind::Unexpected, "writer has been closed"));
            }
            WriteLifecycle::Aborting | WriteLifecycle::Aborted => {
                return Err(Error::new(ErrorKind::Unexpected, "writer has been aborted"));
            }
        }

        if self.requires_abort {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "writer must be aborted after a partially accepted write",
            ));
        }
        Ok(())
    }

    async fn write_chunk(&mut self, mut bs: Buffer) -> Result<usize> {
        let size = bs.len();
        let Some(chunk_size) = self.chunk_size else {
            self.w.write(bs).await?;
            return Ok(size);
        };

        let buffered = self.buffer.len();
        if !self.exact {
            if buffered + size < chunk_size {
                self.buffer.push(bs);
            } else {
                self.flush_buffer(Some(bs)).await?;
            }
            return Ok(size);
        }

        let remaining = if buffered == chunk_size {
            self.flush_buffer(None).await?;
            chunk_size
        } else {
            chunk_size - buffered
        };

        if size <= remaining {
            self.buffer.push(bs);
            return Ok(size);
        }

        // Flush a full chunk before accepting any input from this call. A
        // failed flush leaves only the bytes accepted by earlier calls queued.
        bs.truncate(remaining);
        self.flush_buffer(Some(bs)).await?;
        Ok(remaining)
    }

    async fn flush_buffer(&mut self, tail: Option<Buffer>) -> Result<()> {
        let mut pending = self.buffer.clone();
        if let Some(tail) = tail {
            pending.push(tail);
        }
        self.w.write(pending.collect()).await?;
        self.buffer.clear();
        Ok(())
    }

    fn reader(&self, path: &str, args: OpRead, options: OpReader) -> Result<Reader> {
        let ctx = self
            .ctx
            .as_ref()
            .expect("write context must exist outside tests");
        let reader = ctx.service().read(ctx.context(), path, args.clone())?;
        Ok(Reader::new(ReadContext::new(
            ctx.context().clone(),
            ctx.service().clone(),
            path.to_string(),
            args,
            options,
            reader,
        )))
    }

    async fn stream_range(
        &mut self,
        path: &str,
        args: OpRead,
        options: OpReader,
        range: BytesRange,
        limit: Option<u64>,
    ) -> Result<()> {
        let reader = self.reader(path, args, options)?;
        let mut stream = reader.into_stream_with_limit(range, limit).await?;
        while let Some(mut bs) = stream.try_next().await? {
            while !bs.is_empty() {
                // The caller already admitted this copy, and a retry must be
                // able to continue it even though `pending_copy` is set.
                let n = self.write_chunk(bs.clone()).await?;
                bs.advance(n);
                self.advance_copy_offset(n as u64);
            }
        }
        Ok(())
    }

    fn advance_copy_offset(&mut self, n: u64) {
        let copy = self
            .pending_copy
            .as_mut()
            .expect("copy progress must belong to a pending copy");
        copy.offset += n;
        debug_assert!(copy.offset <= copy.end);
    }

    fn copy_offset(&self) -> u64 {
        self.pending_copy
            .as_ref()
            .expect("pending copy must exist")
            .offset
    }

    /// Copy a source range into this writer, using native execution when possible.
    pub async fn copy_from(
        &mut self,
        path: &str,
        args: OpRead,
        options: OpReader,
        range: BytesRange,
        limit: Option<u64>,
    ) -> Result<()> {
        const MAX_COPY_FROM_SIZE: u64 = 5 * 1024 * 1024 * 1024;

        self.check_active()?;
        self.pending_copy.take_if(|copy| !copy.has_accepted_bytes());

        if let Some(copy) = &self.pending_copy {
            // Only the identical logical call can continue after the writer
            // accepted part of its source range. Any other call would append
            // different bytes after that prefix and invalidate a later retry.
            if !copy.matches(path, &args, range, limit) {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer must be aborted or resume the same copy_from call after a partially accepted copy",
                )
                .set_persistent());
            }
        } else {
            let reader = self.reader(path, args.clone(), options.clone())?;
            // A zero limit reads nothing. Opening the reader still validates
            // the read options and capabilities, matching `read_options`.
            if limit == Some(0) || range.size() == Some(0) {
                return Ok(());
            }

            // Without a limit, the raw range is the effective copy size. With
            // a limit, resolve the range first: the limit may keep a range over
            // 5 GiB or a bounded end past `u64::MAX` within both limits.
            if limit.is_none()
                && let Some(size) = range.size()
                && size > MAX_COPY_FROM_SIZE
            {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "copy_from range exceeds the 5 GiB per-call limit",
                )
                .with_context("size", size));
            }

            // A length represented by u64 can never contain a byte at u64::MAX.
            // Match `Operator::read_inner` by clamping a bounded forward range
            // to the limit and to the bytes addressable from its offset before
            // resolving it.
            let planned_range = match (limit, range) {
                (
                    Some(limit),
                    BytesRange::Range {
                        offset,
                        size: Some(size),
                    },
                ) => {
                    let available = u64::MAX - offset;
                    if available == 0 {
                        return Err(Error::new(
                            ErrorKind::RangeNotSatisfied,
                            "range starts at or beyond the end of the object",
                        )
                        .with_context("offset", offset));
                    }
                    let size = size.min(limit).min(available);
                    if size > MAX_COPY_FROM_SIZE {
                        return Err(Error::new(
                            ErrorKind::RangeNotSatisfied,
                            "copy_from range exceeds the 5 GiB per-call limit",
                        )
                        .with_context("size", size));
                    }
                    BytesRange::new(offset, Some(size))
                }
                (_, range) => range,
            };

            let absolute = reader.parse_into_range(planned_range).await?;
            let end = match limit {
                // A limit bounds the bytes copied from the start of the
                // selected range. Suffix and open-ended ranges were already
                // resolved against the source length above.
                Some(limit) => absolute.end.min(absolute.start.saturating_add(limit)),
                None => absolute.end,
            };
            let size = end - absolute.start;
            if size > MAX_COPY_FROM_SIZE {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "copy_from range exceeds the 5 GiB per-call limit",
                )
                .with_context("size", size));
            }
            if size == 0 {
                // An explicit empty range and an empty suffix selection copy
                // nothing. A non-empty forward selection that starts at or
                // beyond EOF fails like `read_options` with a limit.
                if limit.is_some_and(|limit| limit > 0) && !range.is_suffix() {
                    return Err(Error::new(
                        ErrorKind::RangeNotSatisfied,
                        "range starts at or beyond the end of the object",
                    )
                    .with_context("offset", absolute.start));
                }
                return Ok(());
            }

            self.pending_copy = Some(Box::new(PendingCopy {
                path: path.to_string(),
                args,
                options,
                range,
                limit,
                start: absolute.start,
                end,
                offset: absolute.start,
            }));
        }

        let result = self.drive_copy().await;
        if result.is_err() {
            self.pending_copy.take_if(|copy| !copy.has_accepted_bytes());
        }
        result
    }

    async fn drive_copy(&mut self) -> Result<()> {
        let (path, args, options, range, end, mut offset, limit) = {
            let copy = self
                .pending_copy
                .as_ref()
                .expect("pending copy must exist while driving it");
            (
                copy.path.clone(),
                copy.args.clone(),
                copy.options.clone(),
                copy.range,
                copy.end,
                copy.offset,
                copy.limit,
            )
        };
        let mut remaining = end - offset;

        if remaining == 0 {
            self.pending_copy = None;
            return Ok(());
        }

        // `parse_into_range` resolves suffix and open-ended ranges against the
        // source length, so their planned parts always stay inside the source.
        // A limited forward bounded range may end past EOF, and only streaming
        // accepts EOF after a valid starting byte. A content length hint may be
        // stale, so a limited read that depends on it must also stream instead
        // of sending parts past EOF to native copy.
        let bounded_forward = !range.is_suffix() && range.size().is_some();
        let can_copy_native = self.can_copy_from
            && (limit.is_none() || (!bounded_forward && args.content_length_hint().is_none()));
        let Some(chunk_size) = self.chunk_size.filter(|_| can_copy_native) else {
            self.stream_range(
                &path,
                args,
                options,
                BytesRange::new(offset, Some(remaining)),
                limit.map(|_| remaining),
            )
            .await?;
            self.pending_copy = None;
            return Ok(());
        };

        if self.buffer.len() >= chunk_size {
            self.flush_buffer(None).await?;
        }

        if !self.buffer.is_empty() {
            let boundary_size = (chunk_size as u64 - self.buffer.len() as u64).min(remaining);
            let expected_offset = offset + boundary_size;
            self.stream_range(
                &path,
                args.clone(),
                options.clone(),
                BytesRange::new(offset, Some(boundary_size)),
                limit.map(|_| boundary_size),
            )
            .await?;
            offset = self.copy_offset();
            if offset < expected_offset {
                // The source ended before the planned boundary. Stop before a
                // native copy could overtake the prefix still in `self.buffer`.
                self.pending_copy = None;
                return Ok(());
            }
            remaining = end - offset;
        }

        // Exact chunking keeps a full buffer queued until the next write, so
        // the boundary fill above can leave `chunk_size` bytes unflushed. Flush
        // them now so the buffered prefix is not written after the native
        // copies below.
        if self.buffer.len() >= chunk_size {
            self.flush_buffer(None).await?;
        }

        while remaining >= chunk_size as u64 {
            let part_size = if self.exact {
                chunk_size as u64
            } else {
                remaining
            };
            let part_range = BytesRange::new(offset, Some(part_size));
            self.w.copy_from(&path, args.clone(), part_range).await?;
            offset += part_size;
            remaining -= part_size;
            self.advance_copy_offset(part_size);
        }

        if remaining > 0 {
            self.stream_range(
                &path,
                args,
                options,
                BytesRange::new(offset, Some(remaining)),
                limit.map(|_| remaining),
            )
            .await?;
        }
        self.pending_copy = None;
        Ok(())
    }

    /// Finish the write process.
    pub async fn close(&mut self) -> Result<Metadata> {
        self.check_closable()?;
        self.lifecycle = WriteLifecycle::Closing;
        if !self.buffer.is_empty() {
            self.flush_buffer(None).await?;
        }
        let meta = self.w.close().await?;
        self.lifecycle = WriteLifecycle::Closed;
        Ok(meta)
    }

    /// Abort the write process.
    pub async fn abort(&mut self) -> Result<()> {
        match self.lifecycle {
            WriteLifecycle::Closed => {
                return Err(Error::new(ErrorKind::Unexpected, "writer has been closed"));
            }
            WriteLifecycle::Aborted => return Ok(()),
            WriteLifecycle::Active | WriteLifecycle::Closing | WriteLifecycle::Aborting => {}
        }

        // Transition before clearing buffered/copy state so a failed or canceled
        // abort can only be retried via `abort()` and can never fall back to
        // writing or closing a truncated upload.
        self.lifecycle = WriteLifecycle::Aborting;
        self.buffer.clear();
        self.pending_copy = None;
        self.requires_abort = false;
        self.w.abort().await?;
        self.lifecycle = WriteLifecycle::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use asyncband::mutex::Mutex;
    use bytes::Buf;
    use bytes::BufMut;
    use bytes::Bytes;
    use log::debug;
    use logforth::append::Testing;
    use logforth::filter::rustlog::RustLogFilterBuilder;
    use logforth::layout::TextLayout;
    use pretty_assertions::assert_eq;
    use rand::{Rng, RngExt, rng};
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::Write;

    fn sha256_digest(data: impl AsRef<[u8]>) -> String {
        use std::fmt::Write;

        let digest = Sha256::digest(data);
        let mut output = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut output, "{byte:02x}").expect("writing to String must succeed");
        }
        output
    }

    struct MockWriter {
        buf: Arc<Mutex<Vec<u8>>>,
        write_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl Write for MockWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            let size = bs.len();
            debug!("test_fuzz_exact_buf_writer: flush size: {}", size);

            let mut write_sizes = self.write_sizes.lock().await;
            write_sizes.push(size);

            let mut buf = self.buf.lock().await;
            buf.put(bs);
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct WriteFaults {
        fail_on_write: usize,
        fail_on_copy: usize,
        pending_on_copy: usize,
        can_copy_from: bool,
        read_with_if_match: bool,
        /// Report this content length from `stat` instead of the real one.
        stat_content_length: Option<u64>,
    }

    impl Default for WriteFaults {
        fn default() -> Self {
            Self {
                fail_on_write: usize::MAX,
                fail_on_copy: usize::MAX,
                pending_on_copy: usize::MAX,
                can_copy_from: true,
                read_with_if_match: false,
                stat_content_length: None,
            }
        }
    }

    #[derive(Debug)]
    struct WriteFaultService {
        inner: Servicer,
        faults: WriteFaults,
        copy_ranges: Arc<std::sync::Mutex<Vec<BytesRange>>>,
        copy_pending: Arc<AtomicBool>,
    }

    impl Service for WriteFaultService {
        type Reader = oio::Reader;
        type Writer = WriteFault;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            Capability {
                write_can_multi: true,
                write_multi_min_size: Some(10),
                write_can_copy_from: self.faults.can_copy_from,
                read_with_if_match: self.faults.read_with_if_match,
                ..self.inner.capability()
            }
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            unreachable!()
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            let rp = self.inner.stat(ctx, path, args).await?;
            Ok(match self.faults.stat_content_length {
                Some(content_length) => rp.map_metadata(|meta| {
                    let mut builder = meta.into_builder();
                    builder.set_file(content_length);
                    builder.build()
                }),
                None => rp,
            })
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            Ok(WriteFault {
                writer: self.inner.write(ctx, path, args)?,
                source: self.inner.clone(),
                ctx: ctx.clone(),
                faults: self.faults,
                write_attempts: 0,
                copy_attempts: 0,
                copy_ranges: self.copy_ranges.clone(),
                copy_pending: self.copy_pending.clone(),
            })
        }

        fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
            unreachable!()
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            unreachable!()
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            unreachable!()
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            unreachable!()
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            unreachable!()
        }
    }

    struct WriteFault {
        writer: oio::Writer,
        source: Servicer,
        ctx: OperationContext,
        faults: WriteFaults,
        write_attempts: usize,
        copy_attempts: usize,
        copy_ranges: Arc<std::sync::Mutex<Vec<BytesRange>>>,
        copy_pending: Arc<AtomicBool>,
    }

    impl Write for WriteFault {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            self.write_attempts += 1;
            if self.write_attempts == self.faults.fail_on_write {
                return Err(
                    Error::new(ErrorKind::Unexpected, "injected flush failure").set_temporary()
                );
            }
            self.writer.write(bs).await
        }

        async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
            self.copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned")
                .push(range);
            self.copy_attempts += 1;

            if self.copy_attempts == self.faults.fail_on_copy {
                return Err(
                    Error::new(ErrorKind::Unexpected, "injected copy failure").set_temporary()
                );
            }
            if self.copy_attempts == self.faults.pending_on_copy {
                self.copy_pending.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            if args.if_match().is_some() {
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "source etag mismatch",
                ));
            }

            let reader = self.source.read(&self.ctx, path, args)?;
            let (_, bs) = oio::Read::read(&reader, range).await?;
            self.writer.write(bs).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.writer.close().await
        }

        async fn abort(&mut self) -> Result<()> {
            self.writer.abort().await
        }
    }

    fn with_write_faults(
        op: &Operator,
        faults: WriteFaults,
    ) -> (
        Operator,
        Arc<std::sync::Mutex<Vec<BytesRange>>>,
        Arc<AtomicBool>,
    ) {
        let copy_ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let copy_pending = Arc::new(AtomicBool::new(false));
        let (ctx, inner) = op.clone().into_parts();
        let op = Operator::from_parts(
            ctx,
            Arc::new(WriteFaultService {
                inner,
                faults,
                copy_ranges: copy_ranges.clone(),
                copy_pending: copy_pending.clone(),
            }),
        );
        (op, copy_ranges, copy_pending)
    }

    fn with_write_fault(op: &Operator, fail_on_write: usize) -> Operator {
        with_write_faults(
            op,
            WriteFaults {
                fail_on_write,
                ..Default::default()
            },
        )
        .0
    }

    #[tokio::test]
    async fn test_retry_inexact_write_after_flush_failure() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let mut writer = with_write_fault(&op, 1).writer("target").await?;
        writer.write("abcde").await?;

        let err = writer
            .write("0123456789")
            .await
            .expect_err("first flush fails");
        assert_eq!(err.message(), "injected flush failure");
        writer.write("0123456789").await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"abcde0123456789");
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_close_after_flush_failure() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let mut writer = with_write_fault(&op, 1).writer("target").await?;
        writer.write("abcde").await?;

        let err = writer.close().await.expect_err("first flush fails");
        assert_eq!(err.message(), "injected flush failure");
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"abcde");
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_copy_from_after_flush_failure() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;
        let mut writer = with_write_fault(&op, 1)
            .writer_with("target")
            .chunk(10)
            .await?;
        writer.write("abcdefghij").await?;

        let err = writer
            .copy_from("source", 0..10)
            .await
            .expect_err("first flush fails");
        assert_eq!(err.message(), "injected flush failure");
        writer.copy_from("source", 0..10).await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"abcdefghij0123456789");
        Ok(())
    }

    /// A failed native copy part must not make the retry replay the parts that
    /// already succeeded.
    #[tokio::test]
    async fn test_retry_copy_from_resumes_after_native_part_failure() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let (op, copy_ranges, _) = with_write_faults(
            &op,
            WriteFaults {
                fail_on_copy: 2,
                ..Default::default()
            },
        );
        let mut writer = op.writer_with("target").chunk(10).await?;

        let err = writer
            .copy_from("source", 0..20)
            .await
            .expect_err("second native copy part fails");
        assert_eq!(err.message(), "injected copy failure");

        let err = writer
            .write("rejected")
            .await
            .expect_err("partial copy rejects unrelated writes");
        assert!(err.is_persistent());

        writer.copy_from("source", 0..20).await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"0123456789abcdefghij");
        assert_eq!(
            *copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned"),
            vec![
                BytesRange::new(0, Some(10)),
                BytesRange::new(10, Some(10)),
                BytesRange::new(10, Some(10)),
            ]
        );
        Ok(())
    }

    /// Dropping a copy future after one native part succeeded must keep the
    /// accepted prefix and resume at the pending part.
    #[tokio::test]
    async fn test_cancelled_copy_from_resumes_after_native_part() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let (op, copy_ranges, copy_pending) = with_write_faults(
            &op,
            WriteFaults {
                pending_on_copy: 2,
                ..Default::default()
            },
        );
        let mut writer = op.writer_with("target").chunk(10).await?;

        {
            let mut fut = std::pin::pin!(writer.copy_from("source", 0..20));
            for _ in 0..100 {
                let _ = futures::poll!(&mut fut);
                if copy_pending.load(Ordering::SeqCst) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                copy_pending.load(Ordering::SeqCst),
                "second native copy part must be pending"
            );
        }

        // A partial copy cannot be closed as if it were complete.
        let err = writer
            .close()
            .await
            .expect_err("partial copy must not close");
        assert!(err.is_persistent());

        writer.copy_from("source", 0..20).await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"0123456789abcdefghij");
        assert_eq!(
            *copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned"),
            vec![
                BytesRange::new(0, Some(10)),
                BytesRange::new(10, Some(10)),
                BytesRange::new(10, Some(10)),
            ]
        );
        Ok(())
    }

    /// A streaming fallback must also resume after a failed chunk without
    /// replaying source bytes.
    #[tokio::test]
    async fn test_retry_copy_from_resumes_after_stream_failure() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghijklmnopqrst").await?;
        let (op, _, _) = with_write_faults(
            &op,
            WriteFaults {
                fail_on_write: 2,
                can_copy_from: false,
                ..Default::default()
            },
        );
        let mut writer = op.writer_with("target").chunk(10).await?;
        let options = crate::options::ReadOptions {
            range: (0..30).into(),
            chunk: Some(5),
            ..Default::default()
        };

        let err = writer
            .copy_from_options("source", options.clone())
            .await
            .expect_err("second streamed chunk fails");
        assert_eq!(err.message(), "injected flush failure");

        writer.copy_from_options("source", options).await?;
        writer.close().await?;

        assert_eq!(
            op.read("target").await?.to_vec(),
            b"0123456789abcdefghijklmnopqrst"
        );
        Ok(())
    }

    /// A limited forward bounded range may end past EOF, so it streams instead
    /// of requesting exact native copy parts.
    #[tokio::test]
    async fn test_copy_from_with_limit_streams_bounded_forward_range() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "01234567890123456789").await?;
        let (op, copy_ranges, _) = with_write_faults(&op, WriteFaults::default());
        let mut writer = op.writer_with("target").chunk(10).await?;

        writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    range: (0..25).into(),
                    limit: Some(100),
                    ..Default::default()
                },
            )
            .await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"01234567890123456789");
        assert!(
            copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned")
                .is_empty(),
            "a limited forward bounded range must not use native copy"
        );
        Ok(())
    }

    /// A limited suffix range resolves against the source length, so native
    /// parts stay inside the source and only the remainder streams.
    #[tokio::test]
    async fn test_copy_from_with_limit_keeps_suffix_native_parts() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "01234567890123456789").await?;
        let (op, copy_ranges, _) = with_write_faults(&op, WriteFaults::default());
        let mut writer = op.writer_with("target").chunk(10).await?;

        writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    range: BytesRange::suffix(20),
                    limit: Some(15),
                    ..Default::default()
                },
            )
            .await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"012345678901234");
        assert_eq!(
            *copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned"),
            vec![BytesRange::new(0, Some(10))]
        );
        Ok(())
    }

    /// A retry must resume the identical limited call; a different limit must
    /// not append another prefix to the partially copied destination.
    #[tokio::test]
    async fn test_retry_copy_from_rejects_mismatched_limit() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let (op, copy_ranges, _) = with_write_faults(
            &op,
            WriteFaults {
                fail_on_copy: 2,
                ..Default::default()
            },
        );
        let mut writer = op.writer_with("target").chunk(10).await?;
        let options = crate::options::ReadOptions {
            range: BytesRange::suffix(20),
            limit: Some(20),
            ..Default::default()
        };

        let err = writer
            .copy_from_options("source", options.clone())
            .await
            .expect_err("second native copy part fails");
        assert_eq!(err.message(), "injected copy failure");

        let err = writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    limit: Some(19),
                    ..options.clone()
                },
            )
            .await
            .expect_err("a different limit must not resume the copy");
        assert!(err.is_persistent());

        writer.copy_from_options("source", options).await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"0123456789abcdefghij");
        assert_eq!(
            *copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned"),
            vec![
                BytesRange::new(0, Some(10)),
                BytesRange::new(10, Some(10)),
                BytesRange::new(10, Some(10)),
            ]
        );
        Ok(())
    }

    /// A limited forward selection that starts at or beyond EOF must fail like
    /// `Operator::read_options`, while an explicit empty selection and an empty
    /// suffix selection copy nothing.
    #[tokio::test]
    async fn test_copy_from_with_limit_at_eof_matches_read_options() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;
        op.write("empty", Buffer::new()).await?;

        for (idx, (source, range)) in [
            ("source", BytesRange::new(10, None)),
            ("source", BytesRange::new(11, None)),
            ("source", BytesRange::new(u64::MAX, None)),
            ("empty", BytesRange::default()),
        ]
        .into_iter()
        .enumerate()
        {
            let options = crate::options::ReadOptions {
                range,
                limit: Some(1),
                ..Default::default()
            };
            let read_err = op
                .read_options(source, options.clone())
                .await
                .expect_err("read_options must reject a forward range at EOF");
            assert_eq!(read_err.kind(), ErrorKind::RangeNotSatisfied, "case {idx}");

            let mut writer = op.writer(&format!("target-{idx}")).await?;
            let copy_err = writer
                .copy_from_options(source, options)
                .await
                .expect_err("copy_from_options must reject a forward range at EOF");
            assert_eq!(copy_err.kind(), ErrorKind::RangeNotSatisfied, "case {idx}");
            writer.abort().await?;
        }

        for (idx, (source, range)) in [
            ("source", BytesRange::new(0, Some(0))),
            ("empty", BytesRange::suffix(1)),
        ]
        .into_iter()
        .enumerate()
        {
            let options = crate::options::ReadOptions {
                range,
                limit: Some(1),
                ..Default::default()
            };
            assert!(
                op.read_options(source, options.clone()).await?.is_empty(),
                "case {idx}"
            );

            let target = format!("empty-target-{idx}");
            let mut writer = op.writer(&target).await?;
            writer.copy_from_options(source, options).await?;
            writer.close().await?;
            assert!(op.read(&target).await?.is_empty(), "case {idx}");
        }
        Ok(())
    }

    /// A zero limit copies nothing, but still validates read options,
    /// capabilities, and retry state like `read_options` does.
    #[tokio::test]
    async fn test_copy_from_zero_limit_validates_options() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;

        // The same-file check runs before the limit.
        let mut writer = op.writer("source").await?;
        let err = writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    limit: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect_err("copying a file onto itself must fail");
        assert_eq!(err.kind(), ErrorKind::IsSameFile);
        writer.abort().await?;

        // A missing source copies nothing.
        let mut writer = op.writer("target").await?;
        writer
            .copy_from_options(
                "missing",
                crate::options::ReadOptions {
                    limit: Some(0),
                    ..Default::default()
                },
            )
            .await?;
        writer.close().await?;
        assert!(op.read("target").await?.is_empty());

        // A zero chunk and single-threaded reads do not conflict with a limit.
        for options in [
            crate::options::ReadOptions {
                limit: Some(0),
                chunk: Some(0),
                ..Default::default()
            },
            crate::options::ReadOptions {
                limit: Some(0),
                concurrent: 1,
                ..Default::default()
            },
        ] {
            let mut writer = op.writer("zero").await?;
            writer.copy_from_options("source", options).await?;
            writer.close().await?;
            assert!(op.read("zero").await?.is_empty());
        }

        // A positive chunk or concurrency conflicts with any limit.
        for options in [
            crate::options::ReadOptions {
                limit: Some(0),
                chunk: Some(2),
                ..Default::default()
            },
            crate::options::ReadOptions {
                limit: Some(0),
                concurrent: 2,
                ..Default::default()
            },
        ] {
            let mut writer = op.writer("zero").await?;
            let err = writer
                .copy_from_options("source", options)
                .await
                .expect_err("limit conflicts with chunk or concurrent reads");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            writer.abort().await?;
        }

        // Capability validation runs at `Writer::copy_from_options` even when
        // the limit copies nothing and on bare operators constructed with
        // `Operator::from_parts`.
        let bare = Operator::from_parts(
            OperationContext::default(),
            Arc::new(services::Memory::default().build()?) as Servicer,
        );
        let mut writer = bare.writer("target").await?;
        let err = writer
            .copy_from_options(
                "missing",
                crate::options::ReadOptions {
                    limit: Some(0),
                    if_match: Some("\"e\"".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("unsupported if_match must fail");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        writer.abort().await?;
        Ok(())
    }

    /// A limit bounds the effective copy size before the 5 GiB and
    /// `offset + size` overflow checks, matching `read_options`.
    #[tokio::test]
    async fn test_copy_from_applies_limit_before_size_checks() -> Result<()> {
        const OVER_MAX: u64 = 6 * 1024 * 1024 * 1024;

        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;

        for (idx, (range, limit, expected)) in [
            (
                BytesRange::new(0, Some(OVER_MAX)),
                10,
                b"0123456789".as_slice(),
            ),
            (BytesRange::suffix(OVER_MAX), 10, b"0123456789".as_slice()),
            (BytesRange::new(2, Some(u64::MAX)), 3, b"234".as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            let target = format!("target-{idx}");
            let mut writer = op.writer(&target).await?;
            writer
                .copy_from_options(
                    "source",
                    crate::options::ReadOptions {
                        range,
                        limit: Some(limit),
                        ..Default::default()
                    },
                )
                .await?;
            writer.close().await?;
            assert_eq!(op.read(&target).await?.to_vec(), expected, "case {idx}");
        }

        // A zero limit bypasses the size check entirely.
        for (idx, range) in [
            BytesRange::new(0, Some(OVER_MAX)),
            BytesRange::suffix(OVER_MAX),
            BytesRange::new(u64::MAX - 1, Some(5)),
        ]
        .into_iter()
        .enumerate()
        {
            let target = format!("zero-{idx}");
            let mut writer = op.writer(&target).await?;
            writer
                .copy_from_options(
                    "source",
                    crate::options::ReadOptions {
                        range,
                        limit: Some(0),
                        ..Default::default()
                    },
                )
                .await?;
            writer.close().await?;
            assert!(op.read(&target).await?.is_empty(), "case {idx}");
        }

        // An overflowing bounded end fails without a limit, but stays
        // representable when the limit stops before the overflow.
        let mut writer = op.writer("overflow").await?;
        let err = writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    range: BytesRange::new(u64::MAX - 1, Some(5)),
                    ..Default::default()
                },
            )
            .await
            .expect_err("an unbounded overflow must fail");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
        writer.abort().await?;

        let mut writer = op.writer("clamped").await?;
        let err = writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    range: BytesRange::new(u64::MAX - 1, Some(5)),
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a clamped selection beyond EOF must fail");
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
        writer.abort().await?;
        Ok(())
    }

    /// A boundary fill that reaches EOF early must stop instead of starting a
    /// native copy while the buffered prefix is still unflushed.
    #[tokio::test]
    async fn test_copy_from_boundary_fill_stops_at_early_eof() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123").await?;
        let (op, copy_ranges, _) = with_write_faults(
            &op,
            WriteFaults {
                stat_content_length: Some(20),
                ..Default::default()
            },
        );
        let mut writer = op.writer_with("target").chunk(10).await?;
        writer.write("XYZ").await?;

        writer
            .copy_from_options(
                "source",
                crate::options::ReadOptions {
                    range: BytesRange::suffix(20),
                    limit: Some(20),
                    ..Default::default()
                },
            )
            .await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"XYZ0123");
        assert!(
            copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned")
                .is_empty(),
            "a native copy must not run after the source ends early"
        );
        Ok(())
    }

    /// An overestimated content length hint must not drive native copy parts
    /// past EOF when a limit is set.
    #[tokio::test]
    async fn test_copy_from_with_limit_streams_overestimated_hint() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;
        let (op, copy_ranges, _) = with_write_faults(&op, WriteFaults::default());
        let mut writer = op.writer_with("target").chunk(10).await?;
        let options = crate::options::ReadOptions {
            range: (4_u64..).into(),
            limit: Some(15),
            content_length_hint: Some(20),
            ..Default::default()
        };

        assert_eq!(
            op.read_options("source", options.clone()).await?.to_vec(),
            b"456789"
        );

        writer.copy_from_options("source", options).await?;
        writer.close().await?;

        assert_eq!(op.read("target").await?.to_vec(), b"456789");
        assert!(
            copy_ranges
                .lock()
                .expect("copy range lock must not be poisoned")
                .is_empty(),
            "a stale content length hint must not use native copy"
        );
        Ok(())
    }

    /// Exact chunking may leave a full buffer unflushed when `copy_from` tops it
    /// up to `chunk_size`. The buffered prefix must still be written before any
    /// native `copy_from` parts.
    #[tokio::test]
    async fn test_exact_copy_from_flushes_full_buffer_before_native_copy() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let mut writer = with_write_fault(&op, usize::MAX)
            .writer_with("target")
            .chunk(10)
            .await?;
        writer.write("XYZ").await?;
        writer.copy_from("source", 0..20).await?;
        writer.close().await?;

        assert_eq!(
            op.read("target").await?.to_vec(),
            b"XYZ0123456789abcdefghij"
        );
        Ok(())
    }

    /// A native `copy_from` following a `write` that filled the buffer to
    /// exactly `chunk_size` must not overtake the buffered chunk.
    #[tokio::test]
    async fn test_exact_copy_from_after_full_buffered_chunk() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let mut writer = with_write_fault(&op, usize::MAX)
            .writer_with("target")
            .chunk(10)
            .await?;
        writer.write("ABCDEFGHIJ").await?;
        writer.copy_from("source", 0..20).await?;
        writer.close().await?;

        assert_eq!(
            op.read("target").await?.to_vec(),
            b"ABCDEFGHIJ0123456789abcdefghij"
        );
        Ok(())
    }

    /// Consecutive `copy_from` calls with misaligned ranges must preserve call
    /// order even when a call ends with the buffer holding exactly `chunk_size`
    /// bytes or a partial tail.
    #[tokio::test]
    async fn test_exact_copy_from_consecutive_calls_preserve_order() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghijklmnopqrstuv")
            .await?;
        let mut writer = with_write_fault(&op, usize::MAX)
            .writer_with("target")
            .chunk(10)
            .await?;
        writer.write("XYZ").await?;
        // Fills the buffered prefix to exactly chunk_size with no bytes left
        // for the native path, leaving a full buffer queued for the next call.
        writer.copy_from("source", 0..7).await?;
        // Starts with the full buffer, then copies a native chunk and a tail
        // that leaves 3 bytes buffered.
        writer.copy_from("source", 7..20).await?;
        // Tops the 3 buffered bytes up to a full chunk, leaving 5 bytes
        // buffered again.
        writer.copy_from("source", 20..32).await?;
        // Fills the 5 buffered bytes to a full chunk, then copies a native
        // chunk and a tail while that chunk is still queued.
        writer.copy_from("source", 0..18).await?;
        writer.write("end").await?;
        writer.close().await?;

        assert_eq!(
            op.read("target").await?.to_vec(),
            b"XYZ0123456789abcdefghijklmnopqrstuv0123456789abcdefghend"
        );
        Ok(())
    }

    /// Without an explicit `.chunk()`, `copy_from` uses the service's minimum
    /// multi size in inexact mode; interleaved writes must keep their order.
    #[tokio::test]
    async fn test_inexact_copy_from_preserves_order() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        let mut writer = with_write_fault(&op, usize::MAX).writer("target").await?;
        writer.write("XYZ").await?;
        writer.copy_from("source", 0..20).await?;
        writer.write("end").await?;
        writer.close().await?;

        assert_eq!(
            op.read("target").await?.to_vec(),
            b"XYZ0123456789abcdefghijend"
        );
        Ok(())
    }

    /// A bounded-range `copy_from` that fails before accepting any bytes must
    /// leave the writer usable for subsequent writes, other copies, and close.
    #[tokio::test]
    async fn test_copy_from_zero_bytes_error_keeps_writer_usable() -> Result<()> {
        // Streaming path failing on a missing source with a non-zero offset.
        let op = Operator::new(services::Memory::default())?;
        op.write("other", "0123456789").await?;
        let mut writer = op.writer("target-stream").await?;
        writer.write("pre:").await?;

        let err = writer
            .copy_from("missing", 5..15)
            .await
            .expect_err("missing source must fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        writer.write("mid:").await?;
        writer.copy_from("other", 0..10).await?;
        writer.close().await?;
        assert_eq!(
            op.read("target-stream").await?.to_vec(),
            b"pre:mid:0123456789"
        );

        // Native copy path failing on missing source and ConditionNotMatch.
        let (fault_op, _, _) = with_write_faults(
            &op,
            WriteFaults {
                read_with_if_match: true,
                ..Default::default()
            },
        );
        let mut writer = fault_op.writer_with("target-native").chunk(10).await?;

        let err = writer
            .copy_from("missing", 0..10)
            .await
            .expect_err("missing source must fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        let err = writer
            .copy_from_options(
                "other",
                crate::options::ReadOptions {
                    range: (0..10).into(),
                    if_match: Some("\"stale-etag\"".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("mismatched etag must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        writer.write("abcdefghij").await?;
        writer.copy_from("other", 0..10).await?;
        writer.close().await?;
        assert_eq!(
            op.read("target-native").await?.to_vec(),
            b"abcdefghij0123456789"
        );
        Ok(())
    }

    /// Dropping a `copy_from` future before any bytes are accepted must leave
    /// the writer usable for subsequent writes, other copies, and close.
    #[tokio::test]
    async fn test_copy_from_cancelled_before_accepting_bytes_keeps_writer_usable() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        op.write("other", "abcdefghij").await?;

        // Case 1: write() and close() succeed after cancellation at 0 accepted bytes.
        let (fault_op, _, copy_pending) = with_write_faults(
            &op,
            WriteFaults {
                pending_on_copy: 1,
                ..Default::default()
            },
        );
        let mut writer = fault_op.writer_with("target-write").chunk(10).await?;
        {
            let mut fut = std::pin::pin!(writer.copy_from("source", 0..20));
            for _ in 0..100 {
                let _ = futures::poll!(&mut fut);
                if copy_pending.load(Ordering::SeqCst) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                copy_pending.load(Ordering::SeqCst),
                "first native copy part must be pending"
            );
        }

        writer.write("fresh-data").await?;
        writer.close().await?;
        assert_eq!(op.read("target-write").await?.to_vec(), b"fresh-data");

        // Case 2: copy_from("other") and close() succeed after cancellation at 0 accepted bytes.
        let (fault_op, _, copy_pending) = with_write_faults(
            &op,
            WriteFaults {
                pending_on_copy: 1,
                ..Default::default()
            },
        );
        let mut writer = fault_op.writer_with("target-other").chunk(10).await?;
        {
            let mut fut = std::pin::pin!(writer.copy_from("source", 0..20));
            for _ in 0..100 {
                let _ = futures::poll!(&mut fut);
                if copy_pending.load(Ordering::SeqCst) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                copy_pending.load(Ordering::SeqCst),
                "first native copy part must be pending"
            );
        }

        writer.copy_from("other", 0..10).await?;
        writer.close().await?;
        assert_eq!(op.read("target-other").await?.to_vec(), b"abcdefghij");

        // Case 3: close() directly succeeds after cancellation at 0 accepted bytes.
        let (fault_op, _, copy_pending) = with_write_faults(
            &op,
            WriteFaults {
                pending_on_copy: 1,
                ..Default::default()
            },
        );
        let mut writer = fault_op.writer_with("target-close").chunk(10).await?;
        writer.write("0123456789").await?;
        {
            let mut fut = std::pin::pin!(writer.copy_from("source", 0..20));
            for _ in 0..100 {
                let _ = futures::poll!(&mut fut);
                if copy_pending.load(Ordering::SeqCst) {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                copy_pending.load(Ordering::SeqCst),
                "first native copy part must be pending"
            );
        }

        writer.close().await?;
        assert_eq!(op.read("target-close").await?.to_vec(), b"0123456789");
        Ok(())
    }

    /// Once at least one byte of a `copy_from` call has been accepted, the
    /// writer must still reject unrelated writes, other `copy_from` calls, and
    /// `close` until the same `copy_from` is resumed or `abort` runs.
    #[tokio::test]
    async fn test_copy_from_partial_accept_still_requires_resume_or_abort() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789abcdefghij").await?;
        op.write("other", "abcdefghij").await?;
        let (fault_op, _, _) = with_write_faults(
            &op,
            WriteFaults {
                fail_on_copy: 2,
                ..Default::default()
            },
        );

        let mut writer = fault_op.writer_with("target-abort").chunk(10).await?;
        let err = writer
            .copy_from("source", 0..20)
            .await
            .expect_err("second native copy part fails after first 10 bytes are accepted");
        assert_eq!(err.message(), "injected copy failure");

        let err = writer
            .write("rejected")
            .await
            .expect_err("unrelated write must fail after partial copy");
        assert!(err.is_persistent());

        let err = writer
            .copy_from("other", 0..10)
            .await
            .expect_err("different copy_from must fail after partial copy");
        assert!(err.is_persistent());

        let err = writer
            .close()
            .await
            .expect_err("close must fail after partial copy");
        assert!(err.is_persistent());

        writer.abort().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_retry_exact_write_after_first_flush_failure() -> Result<()> {
        for (prefix, fail_on_write, expected) in [
            ("abcde", 1, "abcde0123456789xyz"),
            ("abcdefghij", 2, "abcdefghij0123456789xyz"),
        ] {
            let op = Operator::new(services::Memory::default())?;
            let mut writer = with_write_fault(&op, fail_on_write)
                .writer_with("target")
                .chunk(10)
                .await?;
            writer.write(prefix).await?;

            let err = writer
                .write("0123456789xyz")
                .await
                .expect_err("flush fails before accepting any bytes of this call");
            assert_eq!(err.message(), "injected flush failure");
            assert!(err.is_temporary());
            writer.write("0123456789xyz").await?;
            writer.close().await?;

            assert_eq!(op.read("target").await?.to_vec(), expected.as_bytes());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sink_position_after_partial_write() -> Result<()> {
        use futures::SinkExt;

        let op = Operator::new(services::Memory::default())?;
        let mut sink = with_write_fault(&op, 2)
            .writer_with("target")
            .chunk(10)
            .await?
            .into_sink();

        let err = sink
            .send(Buffer::from("abcdefghijklmnopqrstuvwxy"))
            .await
            .expect_err("second flush fails");
        assert_eq!(err.message(), "injected flush failure");
        assert_eq!(sink.position(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_exact_write_rejects_replay_after_partial_progress() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let mut writer = with_write_fault(&op, 2)
            .writer_with("target")
            .chunk(10)
            .await?;
        let data = "abcdefghijklmnopqrstuvwxy";

        let err = writer.write(data).await.expect_err("second flush fails");
        assert_eq!(err.message(), "injected flush failure");
        assert!(err.is_persistent());
        assert!(writer.write(data).await.is_err());
        assert!(writer.close().await.is_err());
        writer.abort().await?;

        assert!(!op.exists("target").await?);
        Ok(())
    }

    fn setup() {
        let _ = logforth::starter_log::builder()
            .dispatch(|d| {
                d.filter(RustLogFilterBuilder::from_default_env().build())
                    .append(Testing::default().with_layout(TextLayout::default()))
            })
            .try_apply();
    }

    #[tokio::test]
    async fn test_exact_buf_writer_short_write() -> Result<()> {
        setup();

        let mut rng = rng();
        let mut expected = vec![0; 5];
        rng.fill_bytes(&mut expected);

        let buf = Arc::new(Mutex::new(vec![]));
        let mut w = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: Arc::new(Mutex::new(vec![])),
            }),
            Some(10),
            true,
        );

        let mut bs = Bytes::from(expected.clone());
        while !bs.is_empty() {
            let n = w.write(bs.clone().into()).await?;
            bs.advance(n);
        }

        w.close().await?;

        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_inexact_buf_writer_large_write() -> Result<()> {
        setup();

        let buf = Arc::new(Mutex::new(vec![]));
        let mut w = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: Arc::new(Mutex::new(vec![])),
            }),
            Some(10),
            false,
        );

        let mut rng = rng();
        let mut expected = vec![0; 15];
        rng.fill_bytes(&mut expected);

        let bs = Bytes::from(expected.clone());
        // The MockWriter always returns the first chunk size.
        let n = w.write(bs.into()).await?;
        assert_eq!(expected.len(), n);

        w.close().await?;

        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_inexact_buf_writer_combine_small() -> Result<()> {
        setup();

        let buf = Arc::new(Mutex::new(vec![]));
        let mut w = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: Arc::new(Mutex::new(vec![])),
            }),
            Some(10),
            false,
        );

        let mut rng = rng();
        let mut expected = vec![];

        let mut new_content = |size| {
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);
            expected.extend_from_slice(&content);
            Bytes::from(content)
        };

        // content > chunk size.
        let content = new_content(15);
        assert_eq!(15, w.write(content.into()).await?);
        // content < chunk size.
        let content = new_content(5);
        assert_eq!(5, w.write(content.into()).await?);
        // content > chunk size, but 5 bytes in queue.
        let content = new_content(15);
        // The MockWriter can send all 15 bytes together, so we can only advance 5 bytes.
        assert_eq!(15, w.write(content.clone().into()).await?);

        w.close().await?;

        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_inexact_buf_writer_queue_remaining() -> Result<()> {
        setup();

        let buf = Arc::new(Mutex::new(vec![]));
        let mut w = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: Arc::new(Mutex::new(vec![])),
            }),
            Some(10),
            false,
        );

        let mut rng = rng();
        let mut expected = vec![];

        let mut new_content = |size| {
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);
            expected.extend_from_slice(&content);
            Bytes::from(content)
        };

        // content > chunk size.
        let content = new_content(15);
        assert_eq!(15, w.write(content.into()).await?);
        // content < chunk size.
        let content = new_content(5);
        assert_eq!(5, w.write(content.into()).await?);
        // content < chunk size.
        let content = new_content(3);
        assert_eq!(3, w.write(content.into()).await?);
        // content > chunk size, but can send all chunks in the queue.
        let content = new_content(15);
        assert_eq!(15, w.write(content.clone().into()).await?);

        w.close().await?;

        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));
        Ok(())
    }

    #[tokio::test]
    async fn test_fuzz_exact_buf_writer() -> Result<()> {
        setup();

        let mut rng = rng();
        let mut expected = vec![];

        let buf = Arc::new(Mutex::new(vec![]));
        let buffer_size = rng.random_range(1..10);
        let mut writer = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: Arc::new(Mutex::new(vec![])),
            }),
            Some(buffer_size),
            true,
        );
        debug!("test_fuzz_exact_buf_writer: buffer size: {buffer_size}");

        for _ in 0..1000 {
            let size = rng.random_range(1..20);
            debug!("test_fuzz_exact_buf_writer: write size: {size}");
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);

            let mut bs = Bytes::from(content.clone());
            while !bs.is_empty() {
                let n = writer.write(bs.clone().into()).await?;
                bs.advance(n);
            }
        }
        writer.close().await?;

        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));
        Ok(())
    }

    /// Test that when writing a large buffer in exact mode, it gets split into chunks.
    ///
    /// This test verifies that WriteState correctly handles large buffers by
    /// splitting them into chunks of the configured chunk size, rather than writing
    /// everything at once.
    #[tokio::test]
    async fn test_exact_buf_writer_large_buffer_splits_into_chunks() -> Result<()> {
        setup();

        let chunk_size = 10;
        let large_buffer_size = 25; // 2.5x chunk_size

        let buf = Arc::new(Mutex::new(vec![]));
        let write_sizes = Arc::new(Mutex::new(vec![]));
        let mut writer = WriteState::new(
            Box::new(MockWriter {
                buf: buf.clone(),
                write_sizes: write_sizes.clone(),
            }),
            Some(chunk_size),
            true, // exact mode
        );

        let mut rng = rng();
        let mut expected = vec![0; large_buffer_size];
        rng.fill_bytes(&mut expected);

        let bs = Bytes::from(expected.clone());
        // In exact mode, a large buffer should be written in chunks.
        // We need to call write multiple times until all data is written.
        let mut remaining = bs.clone();
        while !remaining.is_empty() {
            let n = writer.write(remaining.clone().into()).await?;
            remaining.advance(n);
        }

        writer.close().await?;

        // Verify all data was written
        let buf = buf.lock().await;
        assert_eq!(buf.len(), expected.len());
        assert_eq!(sha256_digest(&*buf), sha256_digest(&expected));

        // Verify that writes were split into chunks (except possibly the last one)
        let write_sizes = write_sizes.lock().await;
        // In exact mode, all writes except the last should be exactly chunk_size
        // The last write might be smaller if there's a remainder
        for (i, &size) in write_sizes.iter().enumerate() {
            if i < write_sizes.len() - 1 {
                // All writes except the last should be exactly chunk_size
                assert_eq!(
                    size, chunk_size,
                    "Write {} should be exactly chunk_size {}, but was {}",
                    i, chunk_size, size
                );
            } else {
                // Last write should be <= chunk_size
                assert!(
                    size <= chunk_size,
                    "Last write should be <= chunk_size {}, but was {}",
                    chunk_size,
                    size
                );
            }
        }

        // Verify we got the expected number of writes
        let expected_writes = large_buffer_size.div_ceil(chunk_size);
        assert_eq!(
            write_sizes.len(),
            expected_writes,
            "Expected {} writes, but got {}",
            expected_writes,
            write_sizes.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_multi_zero_capability_bounds() -> Result<()> {
        let test = async {
            let make_ctx = |overrides: &str, chunk: Option<usize>| -> Result<WriteContext> {
                let op = Operator::new(services::Memory::default())?
                    .layer(layers::CapabilityOverrideLayer::from_overrides(overrides)?);
                let mut options = OpWriter::new();
                if let Some(chunk) = chunk {
                    options = options.with_chunk(chunk);
                }
                Ok(WriteContext::new(
                    op.context().clone(),
                    op.service().clone(),
                    "a".to_string(),
                    OpWrite::new(),
                    options,
                ))
            };

            assert_eq!(
                make_ctx("write_multi_max_size=0", Some(4))?.calculate_chunk_size(),
                (Some(4), true)
            );
            assert_eq!(
                make_ctx("write_multi_min_size=0", None)?.calculate_chunk_size(),
                (None, false)
            );
            assert_eq!(
                make_ctx("write_multi_min_size=0,write_multi_max_size=0", Some(4))?
                    .calculate_chunk_size(),
                (Some(4), true)
            );
            assert_eq!(
                make_ctx("write_multi_min_size=8,write_multi_max_size=0", Some(4))?
                    .calculate_chunk_size(),
                (Some(8), true)
            );

            let op = Operator::new(services::Memory::default())?.layer(
                layers::CapabilityOverrideLayer::from_overrides("write_multi_max_size=0")?,
            );
            op.write_with("a", "hello").chunk(4).await?;
            assert_eq!(op.read("a").await?.to_bytes(), "hello");
            Ok::<(), Error>(())
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), test)
            .await
            .expect("write with write_multi_max_size=0 must not hang")?;
        Ok(())
    }

    #[derive(Default)]
    struct LifecycleCalls {
        writes: usize,
        closes: usize,
        aborts: usize,
        close_failures: usize,
        abort_failures: usize,
    }

    struct LifecycleMockWriter {
        calls: Arc<std::sync::Mutex<LifecycleCalls>>,
    }

    impl LifecycleMockWriter {
        fn new(
            close_failures: usize,
            abort_failures: usize,
        ) -> (Self, Arc<std::sync::Mutex<LifecycleCalls>>) {
            let calls = Arc::new(std::sync::Mutex::new(LifecycleCalls {
                close_failures,
                abort_failures,
                ..Default::default()
            }));
            (
                Self {
                    calls: calls.clone(),
                },
                calls,
            )
        }
    }

    impl Write for LifecycleMockWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            self.calls.lock().unwrap().writes += 1;
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            let mut calls = self.calls.lock().unwrap();
            calls.closes += 1;
            if calls.close_failures > 0 {
                calls.close_failures -= 1;
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient close failure").set_temporary()
                );
            }
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            let mut calls = self.calls.lock().unwrap();
            calls.aborts += 1;
            if calls.abort_failures > 0 {
                calls.abort_failures -= 1;
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient abort failure").set_temporary()
                );
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct FailingAbortWrapper {
        inner: Servicer,
        abort_failures: Arc<std::sync::Mutex<usize>>,
        abort_calls: Arc<std::sync::Mutex<usize>>,
        close_calls: Arc<std::sync::Mutex<usize>>,
    }

    struct FailingAbortWriter {
        inner: oio::Writer,
        abort_failures: Arc<std::sync::Mutex<usize>>,
        abort_calls: Arc<std::sync::Mutex<usize>>,
        close_calls: Arc<std::sync::Mutex<usize>>,
    }

    impl Write for FailingAbortWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            self.inner.write(bs).await
        }

        async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
            self.inner.copy_from(path, args, range).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            *self.close_calls.lock().unwrap() += 1;
            self.inner.close().await
        }

        async fn abort(&mut self) -> Result<()> {
            let should_fail = {
                *self.abort_calls.lock().unwrap() += 1;
                let mut remaining = self.abort_failures.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if should_fail {
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient abort failure").set_temporary()
                );
            }
            self.inner.abort().await
        }
    }

    impl Service for FailingAbortWrapper {
        type Reader = oio::Reader;
        type Writer = FailingAbortWriter;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            Ok(FailingAbortWriter {
                inner: self.inner.write(ctx, path, args)?,
                abort_failures: self.abort_failures.clone(),
                abort_calls: self.abort_calls.clone(),
                close_calls: self.close_calls.clone(),
            })
        }

        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    #[tokio::test]
    async fn test_write_state_rejects_operations_after_close() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mock, calls) = LifecycleMockWriter::new(0, 0);
            let mut state = WriteState::new(Box::new(mock), None, false);

            state.write(Buffer::from("hello")).await?;
            state.close().await?;

            let err = state
                .write(Buffer::from("world"))
                .await
                .expect_err("write after close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .write_all(Buffer::from("world"))
                .await
                .expect_err("write_all after close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .copy_from(
                    "source",
                    OpRead::new(),
                    OpReader::new(),
                    BytesRange::new(0, Some(5)),
                    None,
                )
                .await
                .expect_err("copy_from after close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state.close().await.expect_err("second close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .abort()
                .await
                .expect_err("abort after close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let calls = calls.lock().unwrap();
            assert_eq!(calls.writes, 1);
            assert_eq!(calls.closes, 1);
            assert_eq!(calls.aborts, 0);
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_write_state_rejects_operations_after_abort() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mock, calls) = LifecycleMockWriter::new(0, 0);
            let mut state = WriteState::new(Box::new(mock), None, false);

            state.write(Buffer::from("hello")).await?;
            state.abort().await?;
            state.abort().await?;

            let err = state
                .write(Buffer::from("world"))
                .await
                .expect_err("write after abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .write_all(Buffer::from("world"))
                .await
                .expect_err("write_all after abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .copy_from(
                    "source",
                    OpRead::new(),
                    OpReader::new(),
                    BytesRange::new(0, Some(5)),
                    None,
                )
                .await
                .expect_err("copy_from after abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = state
                .close()
                .await
                .expect_err("close after abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let calls = calls.lock().unwrap();
            assert_eq!(calls.writes, 1);
            assert_eq!(calls.closes, 0);
            assert_eq!(calls.aborts, 1);
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_write_state_failed_close_remains_retryable_and_abortable() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mock, calls) = LifecycleMockWriter::new(1, 0);
            let mut state = WriteState::new(Box::new(mock), Some(16), false);
            state.write(Buffer::from("hello")).await?;

            let err = state
                .close()
                .await
                .expect_err("first close must fail transiently");
            assert!(err.is_temporary());

            let err = state
                .write(Buffer::from("late"))
                .await
                .expect_err("write while closing must fail even with chunk buffer");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            state.close().await?;

            let err = state
                .close()
                .await
                .expect_err("close after successful retry must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            {
                let calls = calls.lock().unwrap();
                assert_eq!(calls.writes, 1);
                assert_eq!(calls.closes, 2);
                assert_eq!(calls.aborts, 0);
            }

            let (mock, calls) = LifecycleMockWriter::new(1, 0);
            let mut state = WriteState::new(Box::new(mock), None, false);
            state.write(Buffer::from("hello")).await?;

            let err = state
                .close()
                .await
                .expect_err("first close must fail transiently");
            assert!(err.is_temporary());

            state.abort().await?;

            let err = state
                .close()
                .await
                .expect_err("close after aborting failed close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let calls = calls.lock().unwrap();
            assert_eq!(calls.closes, 1);
            assert_eq!(calls.aborts, 1);
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_writer_failed_abort_after_partial_copy_blocks_write_and_close() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let op = Operator::new(services::Memory::default())?;
            op.write("source", "0123456789abcdefghij").await?;
            let (fault_op, _, _) = with_write_faults(
                &op,
                WriteFaults {
                    can_copy_from: true,
                    fail_on_copy: 2,
                    ..Default::default()
                },
            );

            let abort_failures = Arc::new(std::sync::Mutex::new(1usize));
            let abort_calls = Arc::new(std::sync::Mutex::new(0usize));
            let close_calls = Arc::new(std::sync::Mutex::new(0usize));
            let (ctx, inner) = fault_op.into_parts();
            let wrapped = Operator::from_parts(
                ctx,
                Arc::new(FailingAbortWrapper {
                    inner,
                    abort_failures,
                    abort_calls: abort_calls.clone(),
                    close_calls: close_calls.clone(),
                }),
            );

            let mut writer = wrapped.writer_with("target").chunk(10).await?;
            let err = writer
                .copy_from("source", 0..20)
                .await
                .expect_err("second native copy part must fail");
            assert!(err.is_temporary());

            let err = writer
                .abort()
                .await
                .expect_err("first abort must fail transiently");
            assert!(err.is_temporary());

            let err = writer
                .write("extra")
                .await
                .expect_err("write after failed abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = writer
                .copy_from("source", 0..20)
                .await
                .expect_err("copy_from after failed abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = writer
                .close()
                .await
                .expect_err("close after failed abort must not commit truncated upload");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            writer.abort().await?;
            writer.abort().await?;

            assert_eq!(*abort_calls.lock().unwrap(), 2);
            assert_eq!(*close_calls.lock().unwrap(), 0);
            assert!(!op.exists("target").await?);
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[cfg(feature = "blocking")]
    #[test]
    fn test_blocking_writer_rejects_write_and_close_after_close() -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let bop = {
            let _guard = rt.enter();
            let op = Operator::new(services::Memory::default())?;
            blocking::Operator::new(op)?
        };

        let mut writer = bop.writer("target")?;
        writer.write("hello")?;
        writer.close()?;

        let err = writer
            .close()
            .expect_err("second close on blocking::Writer must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_permanent());

        let err = writer
            .write("world")
            .expect_err("write after close on blocking::Writer must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_permanent());

        assert_eq!(bop.read("target")?.to_bytes(), "hello");
        Ok(())
    }
}
