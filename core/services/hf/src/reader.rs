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

use super::backend::*;
use super::core::{HfCore, HfDownloadMode, XetFileResponse};
use asyncband::once::OnceCell;
use bytes::Buf;
use http::Response;
use opendal_core::raw::*;
use opendal_core::*;
use std::ops::Range;
use xet::xet_session::{SessionError, XetDownloadStream, XetDownloadStreamGroup, XetFileInfo};

pub enum HfReadStream {
    Http(HttpBody),
    Xet(XetDownloadStream),
}

/// Converts an opendal byte range into the `Option<Range<u64>>` the `xet`
/// crate expects: `None` for a full read, `Some(start..end)` otherwise, with
/// an open-ended range (`size` unknown) mapped to `start..u64::MAX`.
fn xet_range(range: BytesRange) -> Option<Range<u64>> {
    if range.is_full() {
        None
    } else {
        let start = range.offset();
        let end = range.size().map(|s| start + s).unwrap_or(u64::MAX);
        Some(start..end)
    }
}

impl HfReadStream {
    /// Build the stream directly from an already-known XET hash + size,
    /// whether that came from a resolve response just parsed in [`HfReader::open`]
    /// or was reused from its cache.
    async fn new_xet(
        group: &XetDownloadStreamGroup,
        hash: &str,
        size: u64,
        range: BytesRange,
    ) -> Result<(RpRead, Self)> {
        let metadata = {
            let metadata = MetadataBuilder::file(size);
            metadata.build()
        };
        let xet_range = xet_range(range);

        let mut stream = group
            .download_stream(XetFileInfo::new(hash.to_string(), size), xet_range)
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to create xet download stream",
                )
                .set_source(err)
            })?;
        stream.start();
        Ok((RpRead::new(metadata), Self::Xet(stream)))
    }

    /// Build the stream from a plain (non-XET) resolve response: it already
    /// carries the range-correct bytes, so there is nothing left to fetch.
    fn new_http(path: &str, resp: Response<HttpBody>) -> Result<(RpRead, Self)> {
        let metadata = parse_into_metadata(path, resp.headers())?;
        Ok((RpRead::new(metadata), Self::Http(resp.into_body())))
    }

    /// Retrieve a resolve response's XET metadata, or hand the response
    /// back untouched if it doesn't carry any -- its body is then the
    /// actual range content, still there for [`Self::new_http`].
    async fn maybe_xet(
        resp: Response<HttpBody>,
    ) -> Result<std::result::Result<XetFileResponse, Response<HttpBody>>> {
        if !resp.headers().contains_key("x-xet-hash") {
            return Ok(Err(resp));
        }
        let (_, mut body) = resp.into_parts();
        let buf = body.to_buffer().await?;
        let info: XetFileResponse =
            serde_json::from_reader(buf.reader()).map_err(new_json_deserialize_error)?;
        Ok(Ok(info))
    }
}

fn map_session_error(e: SessionError) -> Error {
    Error::new(ErrorKind::Unexpected, "xet read error").set_source(e)
}

impl oio::ReadStream for HfReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            Self::Http(body) => body.read().await,
            Self::Xet(stream) => match stream.next().await {
                Ok(Some(bytes)) => Ok(Buffer::from(bytes)),
                Ok(None) => Ok(Buffer::new()),
                Err(e) => Err(map_session_error(e)),
            },
        }
    }
}

/// Reader returned by this backend.
pub struct HfReader {
    backend: HfBackend,
    ctx: OperationContext,
    path: String,
    // `Some((hash, size))`/`None` once this path is known to be (or not be)
    // XET-backed; only consulted in `HfDownloadMode::Xet`. Single-flighted via
    // `get_or_try_init`, so concurrent cold opens share one classifying
    // resolve and every later range reuses it. If not XET-backed, that first
    // resolve's body goes unused and every open falls back to its own fresh
    // resolve -- one wasted request per reader, not per range.
    //
    // Never re-validated for this reader's life, so a file changed mid-read
    // (e.g. a new commit on a floating `main`) can serve stale content --
    // scoped to one reader, not the `Operator`, to bound that window.
    resolved: OnceCell<Option<(String, u64)>>,
    // Built once per reader and reused for every later XET range, seeded
    // from `HfCore`'s cached CAS token. Reuse across concurrent ranges
    // relies on the `xet` crate's documented (not type-enforced) support
    // for many streams per group.
    xet_group: OnceCell<XetDownloadStreamGroup>,
}

impl HfReader {
    pub(super) fn new(backend: HfBackend, ctx: OperationContext, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            resolved: OnceCell::new(),
            xet_group: OnceCell::new(),
        }
    }

    /// Build a XET stream via this reader's cached group, creating it on
    /// first use.
    async fn xet_stream(
        &self,
        core: &HfCore,
        hash: &str,
        size: u64,
        range: BytesRange,
    ) -> Result<(RpRead, HfReadStream)> {
        let group = self
            .xet_group
            .get_or_try_init(|| core.xet_download_group(&self.ctx))
            .await?;
        HfReadStream::new_xet(group, hash, size, range).await
    }

    /// Classify an already-fetched resolve response and dispatch to the
    /// matching stream. Used by [`Self::open`]'s Http-mode branch, which
    /// classifies the same response it fetches bytes from. The Xet-mode
    /// fallback (re-resolving in `HfDownloadMode::Http` after classification
    /// came back non-XET) does not call this -- it never re-checks for
    /// XET metadata on that second resolve, so it goes straight to
    /// `new_http` instead.
    async fn dispatch(
        &self,
        core: &HfCore,
        path: &str,
        range: BytesRange,
        resp: Response<HttpBody>,
    ) -> Result<(RpRead, HfReadStream)> {
        match HfReadStream::maybe_xet(resp).await? {
            Ok(info) => self.xet_stream(core, &info.hash, info.size, range).await,
            Err(resp) => HfReadStream::new_http(path, resp),
        }
    }
}

impl oio::StreamRead for HfReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let core = &self.backend.core;
        let path = self.path.as_str();

        if core.download_mode != HfDownloadMode::Xet {
            // Http mode: resolve() is itself the byte-fetching request, done
            // fresh per range -- there's no separate metadata step to cache.
            let resp = core
                .resolve(&self.ctx, path, range, core.download_mode)
                .await?;
            let (rp, stream) = self.dispatch(core, path, range, resp).await?;
            return Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>));
        }

        // Xet mode: classification is single-flighted, see `resolved`'s
        // doc comment for why that's free for the XET outcome and cheap
        // (one extra request, once per reader) for the NotXet one.
        let classification = self
            .resolved
            .get_or_try_init(|| async {
                let resp = core
                    .resolve(&self.ctx, path, range, HfDownloadMode::Xet)
                    .await?;
                match HfReadStream::maybe_xet(resp).await? {
                    Ok(info) => Ok(Some((info.hash, info.size))),
                    Err(_) => Ok(None),
                }
            })
            .await?;

        let (rp, stream) = match classification {
            Some((hash, size)) => self.xet_stream(core, hash, *size, range).await?,
            None => {
                let resp = core
                    .resolve(&self.ctx, path, range, HfDownloadMode::Http)
                    .await?;
                HfReadStream::new_http(path, resp)?
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::test_utils::{mbpp_operator, testing_dataset_core};
    use super::super::core::HfRepoType;
    use super::super::core::test_utils::create_test_core;
    use super::super::core::{CommitFile, DeletedFile, HfCore};
    use super::*;
    use bytes::Bytes;
    use opendal_core::raw::oio::{ReadStream, StreamRead};
    use std::sync::Arc;

    /// Parquet magic bytes: "PAR1"
    const PARQUET_MAGIC: &[u8] = b"PAR1";

    fn hf_reader(core: HfCore, ctx: OperationContext, path: &str) -> HfReader {
        hf_reader_from_arc(Arc::new(core), ctx, path)
    }

    fn hf_reader_from_arc(core: Arc<HfCore>, ctx: OperationContext, path: &str) -> HfReader {
        let backend = HfBackend { core };
        HfReader::new(backend, ctx, path, OpRead::default())
    }

    #[test]
    fn test_xet_range_conversion() {
        assert_eq!(xet_range(BytesRange::default()), None);
        assert_eq!(xet_range(BytesRange::new(4, Some(4))), Some(4..8));
        assert_eq!(xet_range(BytesRange::new(4, None)), Some(4..u64::MAX));
    }

    #[tokio::test]
    async fn test_http_read_uses_resolve_url() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        let reader = hf_reader(core, ctx, "config.json");

        let (_, mut stream) = reader.open(BytesRange::default()).await?;

        assert_eq!(
            mock_client.get_captured_url(),
            "https://huggingface.co/test-user/test-repo/resolve/main/config.json"
        );
        let chunk = stream.read().await?;
        assert_eq!(chunk.to_bytes(), Bytes::from_static(b"hello"));

        Ok(())
    }

    #[tokio::test]
    async fn test_http_read_returns_metadata() -> Result<()> {
        let (core, ctx, _) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        let reader = hf_reader(core, ctx, "test.txt");

        let (rp, mut stream) = reader.open(BytesRange::default()).await?;
        let metadata = rp.metadata().expect("read metadata must be returned");

        assert_eq!(metadata.mode(), EntryMode::FILE);
        assert_eq!(metadata.content_length(), 5);

        let chunk = stream.read().await?;
        assert_eq!(chunk.to_bytes(), Bytes::from_static(b"hello"));

        Ok(())
    }

    /// Classification (`None` for a non-XET file) is cached, but a non-XET
    /// file has no separate metadata step to skip for its actual bytes: the
    /// classifying resolve's body is discarded and every open, including the
    /// first, fetches its own bytes with its own resolve. So the first open
    /// costs 3 (canonical repo id + classify + fetch) and every later one
    /// costs 1 (fetch only, classification and the canonical repo id already
    /// cached).
    #[tokio::test]
    async fn test_non_xet_reads_cache_classification_but_still_fetch_each_range() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        let reader = hf_reader(core, ctx, "plain.txt");

        let (_, mut s1) = reader.open(BytesRange::new(0, Some(1))).await?;
        s1.read().await?;
        assert_eq!(mock_client.request_count(), 3);

        let (_, mut s2) = reader.open(BytesRange::new(1, Some(1))).await?;
        s2.read().await?;
        assert_eq!(mock_client.request_count(), 4);

        Ok(())
    }

    /// A failed classifying resolve must not permanently cache a failure:
    /// the next `open()` on the same reader should retry rather than being
    /// stuck erroring for the reader's whole lifetime.
    #[tokio::test]
    async fn test_classification_retries_after_resolve_failure() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        let reader = hf_reader(core, ctx, "plain.txt");
        mock_client.fail_next_requests(1);

        // The injected failure hits the canonical repo id lookup, the first
        // request a classifying resolve makes.
        let result = reader.open(BytesRange::new(0, Some(1))).await;
        assert!(
            result.is_err(),
            "a failed classifying resolve must surface as an error"
        );
        assert_eq!(mock_client.request_count(), 1);

        let (_, mut stream) = reader.open(BytesRange::new(0, Some(1))).await?;
        stream.read().await?;
        assert_eq!(
            mock_client.request_count(),
            4,
            "classification must retry (canonical repo id + resolve) then fetch the range (1 more)"
        );

        Ok(())
    }

    /// Same guarantee as the non-XET version below, but for a file that
    /// classifies as XET-backed: concurrent opens must share both the one
    /// classifying resolve and the one `xet_group` build (and, via that
    /// group, the one CAS read-token fetch) rather than each independently
    /// racing to build its own group.
    #[tokio::test]
    async fn test_concurrent_cold_opens_on_xet_file_share_one_group() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        mock_client.set_xet_backed(&"00".repeat(32), 4);
        mock_client.set_xet_token_expires_at(u64::MAX);
        let reader = hf_reader(core, ctx, "xet-file.bin");

        let (r1, r2, r3) = futures::join!(
            reader.open(BytesRange::new(0, Some(1))),
            reader.open(BytesRange::new(1, Some(1))),
            reader.open(BytesRange::new(2, Some(1))),
        );
        r1?;
        r2?;
        r3?;

        // 1 shared canonical repo id lookup + 1 shared classifying resolve +
        // 1 shared xet-read-token fetch for the shared group build. Fetching
        // actual bytes from the group would need a real CAS server, so this
        // test only covers open().
        assert_eq!(mock_client.request_count(), 3);

        Ok(())
    }

    /// Documents a known sharp edge in the single-flighted classification:
    /// the `Range` header on the one shared classifying resolve comes from
    /// whichever concurrent `open()` call happens to win `get_or_try_init`'s
    /// race, not from the caller that ends up consuming the result. All
    /// racing callers get that winner's classification regardless of their
    /// own requested range. See `resolved`'s field doc and the review notes
    /// on this PR -- not yet confirmed against the real HF service, so no
    /// fix has been applied; this test only pins today's behavior so a
    /// future change here is deliberate.
    #[tokio::test]
    async fn test_concurrent_cold_opens_classify_with_one_racing_callers_range() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        mock_client.set_xet_backed(&"00".repeat(32), 4);
        mock_client.set_xet_token_expires_at(u64::MAX);
        let reader = hf_reader(core, ctx, "xet-file.bin");

        let ranges = [
            BytesRange::new(0, Some(1)),
            BytesRange::new(10, Some(1)),
            BytesRange::new(20, Some(1)),
        ];
        let (r1, r2, r3) = futures::join!(
            reader.open(ranges[0]),
            reader.open(ranges[1]),
            reader.open(ranges[2]),
        );
        r1?;
        r2?;
        r3?;

        // Exactly one classifying resolve fires (see
        // `test_concurrent_cold_opens_on_xet_file_share_one_group`), and its
        // `Range` header must be exactly one of the three racing callers'
        // own ranges -- not absent, and not some range not asked for by any
        // of them.
        let captured = mock_client
            .get_captured_classify_range_header()
            .expect("the shared classifying resolve must carry a Range header");
        assert!(
            ranges.iter().any(|r| r.to_header() == captured),
            "the shared classifying resolve's Range header ({captured:?}) must match \
             one of the racing callers' own ranges"
        );

        Ok(())
    }

    /// Concurrent opens on a cold reader share exactly one classifying
    /// resolve (`object_store::get_ranges` drives up to 8 by default) --
    /// they must not each independently probe the path before finding out
    /// it isn't XET-backed. Each still fetches its own range afterward.
    #[tokio::test]
    async fn test_concurrent_cold_opens_share_one_classifying_resolve() -> Result<()> {
        let (core, ctx, mock_client) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );
        let reader = hf_reader(core, ctx, "plain.txt");

        let (r1, r2, r3) = futures::join!(
            reader.open(BytesRange::new(0, Some(1))),
            reader.open(BytesRange::new(1, Some(1))),
            reader.open(BytesRange::new(2, Some(1))),
        );
        r1?.1.read().await?;
        r2?.1.read().await?;
        r3?.1.read().await?;

        // 1 canonical repo id lookup + 1 shared classifying resolve + 3 individual fetches.
        assert_eq!(mock_client.request_count(), 5);

        Ok(())
    }

    /// Exercises the XET download code path against a public dataset known to
    /// have XET-stored files. Behavior tests cannot reliably cover this path
    /// because the test dataset may not contain any XET files.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_parquet() {
        let op = mbpp_operator();
        let data = op
            .read("full/train-00000-of-00001.parquet")
            .await
            .expect("xet read should succeed");
        let bytes = data.to_vec();
        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..4], PARQUET_MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);
    }

    /// Verifies that a non-XET file (plain git blob) read in Xet mode falls back
    /// to the HTTP body path rather than erroring. Uploads a small file via the
    /// git commit API (which does not go through XET), then reads it back.
    /// Requires HF_OPENDAL_DATASET and HF_OPENDAL_TOKEN.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_xet_mode_falls_back_to_http_for_non_xet_file() {
        use base64::Engine;

        let core = testing_dataset_core();
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
            opendal_http_transport_reqwest::ReqwestTransport::default(),
        ));
        let content = b"non-xet fallback test content";
        let path = "tests/non-xet-fallback.txt";

        core.commit_git(
            &ctx,
            vec![CommitFile {
                path: path.to_string(),
                content: base64::prelude::BASE64_STANDARD.encode(content),
                encoding: "base64".to_string(),
            }],
            vec![],
            vec![],
            vec![],
        )
        .await
        .expect("commit should succeed");

        let reader = hf_reader_from_arc(core, ctx, path);
        let (_, mut stream) = reader
            .open(BytesRange::default())
            .await
            .expect("reading non-XET file in Xet mode should succeed via HTTP fallback");

        let mut buf = Vec::new();
        loop {
            let chunk: Buffer = stream.read().await.expect("read chunk should succeed");
            if chunk.is_empty() {
                break;
            }
            buf.extend_from_slice(&chunk.to_bytes());
        }
        assert_eq!(buf, content);

        let core = &reader.backend.core;
        core.commit_git(
            &reader.ctx,
            vec![],
            vec![],
            vec![DeletedFile {
                path: path.to_string(),
            }],
            vec![],
        )
        .await
        .ok();
    }

    /// Exercises XET range reads (XetDownloadStream with a byte range).
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_range() {
        let op = mbpp_operator();
        let data = op
            .read_with("full/train-00000-of-00001.parquet")
            .range(0..4)
            .await
            .expect("xet range read should succeed");
        let bytes = data.to_vec();
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes, PARQUET_MAGIC);
    }

    /// Exercises the group-reuse path for real: two ranges fetched through
    /// the same `Reader` (mirroring `object_store::get_ranges`) must both
    /// return correct bytes even though only the first call resolves the
    /// XET hash and builds the CAS download group.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_multiple_ranges_on_one_reader() {
        let op = mbpp_operator();
        let reader = op
            .reader_with("full/train-00000-of-00001.parquet")
            .await
            .expect("opening a reader should succeed");

        let bufs = reader
            .fetch(vec![0..4, 4..8])
            .await
            .expect("fetching two ranges on one reader should succeed");
        assert_eq!(bufs.len(), 2);
        assert_eq!(bufs[0].to_vec(), PARQUET_MAGIC);
        assert_eq!(bufs[1].len(), 4);
    }
}
