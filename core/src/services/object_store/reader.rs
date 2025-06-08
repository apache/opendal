use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use object_store::GetRange;
use object_store::GetResult;
use object_store::ObjectStore;

use crate::raw::*;
use crate::services::object_store::error::parse_error;
use crate::Error;
use crate::ErrorKind;
use crate::*;
use futures::FutureExt;

/// ObjectStore reader
pub struct ObjectStoreReader {
    bytes: Option<Bytes>,
    meta: object_store::ObjectMeta,
    args: OpRead,
}

impl ObjectStoreReader {
    pub(crate) async fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: &str,
        args: OpRead,
    ) -> Result<Self> {
        let path = object_store::path::Path::from(path);
        let opts = parse_read_args(&args)?;
        let result = store.get_opts(&path, opts).await.map_err(parse_error)?;
        let meta = result.meta.clone();
        let bytes = result.bytes().await.map_err(parse_error)?;
        Ok(Self {
            bytes: Some(bytes),
            meta,
            args,
        })
    }

    pub(crate) fn rp(&self) -> RpRead {
        let mut rp = RpRead::new().with_size(Some(self.meta.size));
        if !self.args.range().is_full() {
            let range = self.args.range();
            let size = match range.size() {
                Some(size) => size,
                None => self.meta.size,
            };
            rp = rp.with_range(Some(
                BytesContentRange::default().with_range(range.offset(), range.offset() + size - 1),
            ));
        }
        rp
    }
}

impl oio::Read for ObjectStoreReader {
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        async {
            let bytes = match self.bytes.take() {
                Some(bytes) => bytes,
                None => return Err(Error::new(ErrorKind::Unexpected, "no bytes to read")),
            };
            Ok(Buffer::from(bytes))
        }
        .boxed()
    }
}

fn parse_read_args(args: &OpRead) -> Result<object_store::GetOptions> {
    let mut options = object_store::GetOptions::default();

    if let Some(version) = args.version() {
        options.version = Some(version.to_string());
    }

    if let Some(if_match) = args.if_match() {
        options.if_match = Some(if_match.to_string());
    }

    if let Some(if_none_match) = args.if_none_match() {
        options.if_none_match = Some(if_none_match.to_string());
    }

    if let Some(if_modified_since) = args.if_modified_since() {
        options.if_modified_since = Some(if_modified_since);
    }

    if let Some(if_unmodified_since) = args.if_unmodified_since() {
        options.if_unmodified_since = Some(if_unmodified_since);
    }

    if !args.range().is_full() {
        let range = args.range();
        match range.size() {
            Some(size) => {
                options.range = Some(GetRange::Bounded(range.offset()..range.offset() + size));
            }
            None => {
                options.range = Some(GetRange::Offset(range.offset()));
            }
        }
    }

    Ok(options)
}
