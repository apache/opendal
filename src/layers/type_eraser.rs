// Copyright 2023 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// TypeEraseLayer will erase the types on internal accesoor.
pub struct TypeEraseLayer;

impl<A: Accessor> Layer<A> for TypeEraseLayer {
    type LayeredAccessor = TypeEraseAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        let meta = inner.metadata();
        TypeEraseAccessor {
            meta,
            inner: Arc::new(inner),
        }
    }
}

/// Provide reader wrapper for backend.
pub struct TypeEraseAccessor<A: Accessor> {
    meta: AccessorMetadata,
    inner: Arc<A>,
}

impl<A: Accessor> Debug for TypeEraseAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Accessor> TypeEraseAccessor<A> {
    async fn erase_reader(&self, path: &str, args: OpRead) -> Result<(RpRead, output::Reader)> {
        let (seekable, streamable) = (
            self.meta.hints().contains(AccessorHint::ReadIsSeekable),
            self.meta.hints().contains(AccessorHint::ReadIsStreamable),
        );

        match (seekable, streamable) {
            (true, true) => {
                let (rp, r) = self.inner.read(path, args).await?;
                Ok((rp, Box::new(r)))
            }
            (false, true) => {
               match (args.range().offset(), args.range().size()) {
                    (Some(offset), Some(size)) => {
                       let r= output::into_reader::by_range(self.inner.clone(), path, offset, size);

                         Ok((RpRead::new(0), Box::new(r)))
                    }
                    (Some(offset), _) => {
                        let r = output::into_reader::by_offset(self.inner.clone(), path, offset);

                         Ok((RpRead::new(0), Box::new(r)))
                    }
                    (None, Some(size)) => {
                        let om = self.inner.stat(path, OpStat::new()).await?.into_metadata();
                        let total_size = om.content_length();
                        let (offset, size) = if size > total_size {
                            (0, total_size)
                        } else {
                            (total_size - size, size)
                        };
                        let r= output::into_reader::by_range(self.inner.clone(), path, offset, size);

                         Ok((RpRead::new(0), Box::new(r)))
                    },
                    (None, None) => {
                        let r = output::into_reader::by_offset(self.inner.clone(), path, 0);

                         Ok((RpRead::new(0), Box::new(r)))
                    }
               }
            }
            (true, false) => {
                let (rp, r) = self.inner.read(path, args).await?;
                let r = output::into_reader::as_streamable(r, 256 * 1024);
                 Ok((rp, Box::new(r)))
            }
            (false, false) => unreachable!("reader is neither seekable nor streamable, please check if service {} implemented correctly", self.meta.scheme()),
        }
    }

    fn erase_blokcing_reader(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, output::BlockingReader)> {
        let (seekable, streamable) = (
            self.meta.hints().contains(AccessorHint::ReadIsSeekable),
            self.meta.hints().contains(AccessorHint::ReadIsStreamable),
        );

        match (seekable, streamable) {
            (true, true) => {
                let (rp, r) = self.inner.blocking_read(path, args)?;
                Ok((rp, Box::new(r)))
            }
            (true, false) => {
                let (rp, r) = self.inner.blocking_read(path, args)?;
                let r = output::into_blocking_reader::as_iterable(r, 256 * 1024);
                Ok((rp, Box::new(r)))
            }
            (false, _) => Err(Error::new(
                ErrorKind::Unsupported,
                "non seekable blocking reader is not supported",
            )),
        }
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for TypeEraseAccessor<A> {
    type Inner = A;
    type Reader = output::Reader;
    type BlockingReader = output::BlockingReader;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.erase_reader(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.erase_blokcing_reader(path, args)
    }
}
