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

//! This mod provides compress support for BytesWrite and decompress support for BytesRead.

use std::io::Result;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_compression::codec::{BrotliDecoder, Decode, GzipDecoder};
use async_compression::util::PartialBuffer;
use futures::io::BufReader;
use futures::io::{AsyncBufRead, AsyncBufReadExt};
use futures::ready;
use log::debug;
use pin_project::pin_project;

use crate::BytesRead;
use crate::BytesReader;

/// CompressAlgorithm represents all compress algorithm that OpenDAL supports.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum CompressAlgorithm {
    /// [Brotli](https://github.com/google/brotli) compression format.
    Brotli,
    /// [bzip2](http://sourceware.org/bzip2/) compression format.
    Bz2,
    /// [Deflate](https://datatracker.ietf.org/doc/html/rfc1951) Compressed Data Format.
    ///
    /// Similar to [`CompressAlgorithm::Gzip`] and [`CompressAlgorithm::Zlib`]
    Deflate,
    /// [Gzip](https://datatracker.ietf.org/doc/html/rfc1952) compress format.
    ///
    /// Similar to [`CompressAlgorithm::Deflate`] and [`CompressAlgorithm::Zlib`]
    Gzip,
    /// [LZMA](https://www.7-zip.org/sdk.html) compress format.
    Lzma,
    /// [Xz](https://tukaani.org/xz/) compress format, the successor of [`CompressAlgorithm::Lzma`].
    Xz,
    /// [Zlib](https://datatracker.ietf.org/doc/html/rfc1950) compress format.
    ///
    /// Similar to [`CompressAlgorithm::Deflate`] and [`CompressAlgorithm::Gzip`]
    Zlib,
    /// [Zstd](https://github.com/facebook/zstd) compression algorithm
    Zstd,
}

impl CompressAlgorithm {
    /// Get the file extension of this compress algorithm.
    pub fn extension(&self) -> &str {
        match self {
            CompressAlgorithm::Brotli => "br",
            CompressAlgorithm::Bz2 => "bz2",
            CompressAlgorithm::Deflate => "deflate",
            CompressAlgorithm::Gzip => "gz",
            CompressAlgorithm::Lzma => "lzma",
            CompressAlgorithm::Xz => "xz",
            CompressAlgorithm::Zlib => "zl",
            CompressAlgorithm::Zstd => "zstd",
        }
    }

    /// Create CompressAlgorithm from file extension.
    ///
    /// If the file extension is not supported, `None` will be return instead.
    pub fn from_extension(ext: &str) -> Option<CompressAlgorithm> {
        match ext {
            "br" => Some(CompressAlgorithm::Brotli),
            "bz2" => Some(CompressAlgorithm::Bz2),
            "deflate" => Some(CompressAlgorithm::Deflate),
            "gz" => Some(CompressAlgorithm::Gzip),
            "lzma" => Some(CompressAlgorithm::Lzma),
            "xz" => Some(CompressAlgorithm::Xz),
            "zl" => Some(CompressAlgorithm::Zlib),
            "zstd" => Some(CompressAlgorithm::Zstd),
            _ => None,
        }
    }

    /// Create CompressAlgorithm from file path.
    ///
    /// If the extension in file path is not supported, `None` will be return instead.
    pub fn from_path(path: &str) -> Option<CompressAlgorithm> {
        let ext = PathBuf::from(path)
            .extension()
            .map(|s| s.to_string_lossy())?
            .to_string();

        CompressAlgorithm::from_extension(&ext)
    }
}

#[derive(Debug)]
enum DecompressState {
    Reading,
    Decoding,
    Finishing,
    Done,
}

#[derive(Debug)]
#[pin_project]
struct DecompressReader<R: BytesRead, D: Decode> {
    #[pin]
    reader: BufReader<R>,
    decoder: D,
    state: DecompressState,
    multiple_members: bool,
}

impl<R: BytesRead, D: Decode> DecompressReader<R, D> {
    pub fn new(reader: R, decoder: D) -> Self {
        Self {
            reader: BufReader::new(reader),
            decoder,
            state: DecompressState::Decoding,
            multiple_members: false,
        }
    }
}

impl<R: BytesRead, D: Decode> futures::io::AsyncRead for DecompressReader<R, D> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut output = PartialBuffer::new(buf);
        let mut this = self.project();

        loop {
            *this.state = match this.state {
                DecompressState::Decoding => {
                    let input = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
                    if input.is_empty() {
                        // Avoid attempting to reinitialise the decoder if the reader
                        // has returned EOF.
                        *this.multiple_members = false;
                        DecompressState::Finishing
                    } else {
                        let mut input = PartialBuffer::new(input);
                        let done = this.decoder.decode(&mut input, &mut output)?;
                        let len = input.written().len();
                        this.reader.as_mut().consume(len);
                        if done {
                            DecompressState::Finishing
                        } else {
                            DecompressState::Decoding
                        }
                    }
                }

                DecompressState::Finishing => {
                    if this.decoder.finish(&mut output)? {
                        if *this.multiple_members {
                            this.decoder.reinit()?;
                            DecompressState::Reading
                        } else {
                            DecompressState::Done
                        }
                    } else {
                        DecompressState::Finishing
                    }
                }

                DecompressState::Done => DecompressState::Done,

                DecompressState::Reading => {
                    let input = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
                    if input.is_empty() {
                        DecompressState::Done
                    } else {
                        DecompressState::Decoding
                    }
                }
            };

            if let DecompressState::Done = *this.state {
                return Poll::Ready(Ok(output.written().len()));
            }
            if output.unwritten().is_empty() {
                return Poll::Ready(Ok(output.written().len()));
            }
        }
    }
}

#[derive(Debug)]
struct DecompressDecoder<R: AsyncBufRead + Unpin, D: Decode> {
    reader: R,
    decoder: D,
    multiple_members: bool,
}

impl<R: AsyncBufRead + Unpin, D: Decode> DecompressDecoder<R, D> {
    pub fn new(reader: R, decoder: D) -> Self {
        Self {
            reader,
            decoder,
            multiple_members: false,
        }
    }

    pub async fn fill_buf(&mut self) -> Result<&[u8]> {
        self.reader.fill_buf().await
    }

    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<(DecompressState, usize)> {
        // If input is empty, inner reader must reach EOF, return directly.
        if input.is_empty() {
            debug!("input is empty, return directly");
            // Avoid attempting to reinitialise the decoder if the reader
            // has returned EOF.
            self.multiple_members = false;
            return Ok((DecompressState::Finishing, 0));
        }

        let mut input = PartialBuffer::new(input);
        let mut output = PartialBuffer::new(output);
        let done = self.decoder.decode(&mut input, &mut output)?;
        let len = input.written().len();
        debug!("advance reader with amt {}", len);
        Pin::new(&mut self.reader).consume(len);

        if done {
            Ok((DecompressState::Finishing, output.written().len()))
        } else {
            Ok((DecompressState::Reading, output.written().len()))
        }
    }

    pub fn finish(&mut self, output: &mut [u8]) -> Result<(DecompressState, usize)> {
        let mut output = PartialBuffer::new(output);
        let done = self.decoder.finish(&mut output)?;
        if done {
            if self.multiple_members {
                self.decoder.reinit()?;
                Ok((DecompressState::Reading, output.written().len()))
            } else {
                Ok((DecompressState::Done, output.written().len()))
            }
        } else {
            Ok((DecompressState::Finishing, output.written().len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::codec::{Decode, ZlibDecoder as ZlibCodec};
    use bytes::BufMut;
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use futures::io::Cursor;
    use futures::AsyncReadExt;
    use log::debug;
    use rand::prelude::*;
    use std::io;
    use std::io::Result;
    use std::io::Write;

    #[tokio::test]
    async fn test_decompress_decode_zlib() -> Result<()> {
        env_logger::init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);
        debug!("raw_content size: {}", content.len());

        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(&content)?;
        let compressed_content = e.finish()?;
        debug!("compressed_content size: {}", compressed_content.len());

        let br = BufReader::new(Cursor::new(compressed_content));
        let mut cr = DecompressDecoder::new(br, ZlibCodec::new());

        let mut result = vec![0; 16 * 1024 * 1024];
        let mut cnt = 0;
        let mut state = DecompressState::Reading;
        let mut buf = Vec::new();
        loop {
            let (_, output) = result.split_at_mut(cnt);

            match state {
                DecompressState::Reading => {
                    debug!("start reading");
                    buf = cr.fill_buf().await?.to_vec();
                    debug!("read data: {}", buf.len());
                    state = DecompressState::Decoding
                }
                DecompressState::Decoding => unsafe {
                    debug!("start decoding from buf {} to output {}", buf.len(), cnt);
                    let (decode_state, written) = cr.decode(&buf, output)?;
                    debug!("decoded from buf {} as output {}", buf.len(), written);
                    state = decode_state;
                    cnt += written;
                },
                DecompressState::Finishing => {
                    debug!("start finishing to output {}", cnt);
                    let (finish_state, written) = cr.finish(output)?;
                    debug!("finished from buf {} as output {}", buf.len(), written);
                    state = finish_state;
                    cnt += written;
                }
                DecompressState::Done => {
                    debug!("done");
                    break;
                }
            }
        }

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_zlib() -> Result<()> {
        env_logger::init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);
        debug!("raw_content size: {}", content.len());

        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(&content)?;
        let compressed_content = e.finish()?;
        debug!("compressed_content size: {}", compressed_content.len());

        let mut cr = DecompressReader::new(Cursor::new(compressed_content), ZlibCodec::new());

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }
}
