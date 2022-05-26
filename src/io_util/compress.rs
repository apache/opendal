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
use std::task::Context;
use std::task::Poll;

use async_compression::codec::BrotliDecoder;
use async_compression::codec::BzDecoder;
use async_compression::codec::Decode;
use async_compression::codec::DeflateDecoder;
use async_compression::codec::GzipDecoder;
use async_compression::codec::LzmaDecoder;
use async_compression::codec::XzDecoder;
use async_compression::codec::ZlibDecoder;
use async_compression::codec::ZstdDecoder;
use async_compression::util::PartialBuffer;
use futures::io::AsyncBufRead;
use futures::io::AsyncBufReadExt;
use futures::io::BufReader;
use futures::ready;
use log::debug;
use pin_project::pin_project;

use crate::BytesRead;

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

impl From<CompressAlgorithm> for DecompressDecoder {
    fn from(v: CompressAlgorithm) -> Self {
        match v {
            CompressAlgorithm::Brotli => DecompressDecoder::Brotli(Box::new(BrotliDecoder::new())),
            CompressAlgorithm::Bz2 => DecompressDecoder::Bz2(BzDecoder::new()),
            CompressAlgorithm::Deflate => DecompressDecoder::Deflate(DeflateDecoder::new()),
            CompressAlgorithm::Gzip => DecompressDecoder::Gzip(GzipDecoder::new()),
            CompressAlgorithm::Lzma => DecompressDecoder::Lzma(LzmaDecoder::new()),
            CompressAlgorithm::Xz => DecompressDecoder::Xz(XzDecoder::new()),
            CompressAlgorithm::Zlib => DecompressDecoder::Zlib(ZlibDecoder::new()),
            CompressAlgorithm::Zstd => DecompressDecoder::Zstd(ZstdDecoder::new()),
        }
    }
}

/// DecompressDecoder contains all decoders that opendal supports.
///
/// # Example
///
/// Please use `CompressAlgorithm.into()` to create a new decoder
///
/// ```
/// use opendal::io_util::CompressAlgorithm;
/// use opendal::io_util::DecompressDecoder;
///
/// let de: DecompressDecoder = CompressAlgorithm::Zstd.into();
/// ```
#[derive(Debug)]
pub enum DecompressDecoder {
    /// BrotliDecoder is too large that is 2592 bytes
    /// Wrap into box to reduce the total size of the enum
    Brotli(Box<BrotliDecoder>),
    Bz2(BzDecoder),
    Deflate(DeflateDecoder),
    Gzip(GzipDecoder),
    Lzma(LzmaDecoder),
    Xz(XzDecoder),
    Zlib(ZlibDecoder),
    Zstd(ZstdDecoder),
}

impl Decode for DecompressDecoder {
    fn reinit(&mut self) -> Result<()> {
        match self {
            DecompressDecoder::Brotli(v) => v.reinit(),
            DecompressDecoder::Bz2(v) => v.reinit(),
            DecompressDecoder::Deflate(v) => v.reinit(),
            DecompressDecoder::Gzip(v) => v.reinit(),
            DecompressDecoder::Lzma(v) => v.reinit(),
            DecompressDecoder::Xz(v) => v.reinit(),
            DecompressDecoder::Zlib(v) => v.reinit(),
            DecompressDecoder::Zstd(v) => v.reinit(),
        }
    }

    fn decode(
        &mut self,
        input: &mut PartialBuffer<impl AsRef<[u8]>>,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressDecoder::Brotli(v) => v.decode(input, output),
            DecompressDecoder::Bz2(v) => v.decode(input, output),
            DecompressDecoder::Deflate(v) => v.decode(input, output),
            DecompressDecoder::Gzip(v) => v.decode(input, output),
            DecompressDecoder::Lzma(v) => v.decode(input, output),
            DecompressDecoder::Xz(v) => v.decode(input, output),
            DecompressDecoder::Zlib(v) => v.decode(input, output),
            DecompressDecoder::Zstd(v) => v.decode(input, output),
        }
    }

    fn flush(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressDecoder::Brotli(v) => v.flush(output),
            DecompressDecoder::Bz2(v) => v.flush(output),
            DecompressDecoder::Deflate(v) => v.flush(output),
            DecompressDecoder::Gzip(v) => v.flush(output),
            DecompressDecoder::Lzma(v) => v.flush(output),
            DecompressDecoder::Xz(v) => v.flush(output),
            DecompressDecoder::Zlib(v) => v.flush(output),
            DecompressDecoder::Zstd(v) => v.flush(output),
        }
    }

    fn finish(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        match self {
            DecompressDecoder::Brotli(v) => v.finish(output),
            DecompressDecoder::Bz2(v) => v.finish(output),
            DecompressDecoder::Deflate(v) => v.finish(output),
            DecompressDecoder::Gzip(v) => v.finish(output),
            DecompressDecoder::Lzma(v) => v.finish(output),
            DecompressDecoder::Xz(v) => v.finish(output),
            DecompressDecoder::Zlib(v) => v.finish(output),
            DecompressDecoder::Zstd(v) => v.finish(output),
        }
    }
}

/// DecompressState is that decode state during decompress.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DecompressState {
    /// Reading means there is no data to be consume, we need to fetch more.
    ///
    /// We need to call `DecompressReader::fetch()`.
    Reading,
    /// Decoding means data is ready.
    ///
    /// We need to call `DecompressReader::decode()`
    Decoding,
    /// Finishing means all data has been consumed.
    ///
    /// We need to call `DecompressReader::finish()` to flush them into output.
    Finishing,
    /// Done means the whole process of decompress is done.
    ///
    /// We should not call any function of `DecompressReader` anymore.
    Done,
}

/// DecompressReader provides decompress support for opendal.
///
/// There are two ways to do decompress: users can decide where `decode` happen.
///
/// - In async runtime: `decode` happen inside `poll_read` (will block the runtime)
/// - In blocking thread: `decode` happen inside a blocking thread (user need to handle the decompress logic)
///
/// Those two way can't be used at the same time.
///
/// # In async runtime
///
/// This way is much more simple: Users can use `DecompressReader` as `AsyncRead`.
///
/// ```no_run
/// use futures::io::Cursor;
/// use opendal::io_util::DecompressReader;
/// use opendal::io_util::CompressAlgorithm;
/// # use std::io::Result;
/// # use futures::AsyncReadExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let content = vec![0; 16 * 1024 * 1024];
/// let mut cr = DecompressReader::new(Cursor::new(content), CompressAlgorithm::Gzip);
/// let mut result = vec![];
/// cr.read_to_end(&mut result).await?;
/// # Ok(())
/// }
/// ```
///
/// # In blocking thread
///
/// Do `decode` inside async runtime will blocking the async runtime. To avoid this happen, we can move the `decode` process to blocking thread.
///
/// We need to handle the `DecompressState` by ourselves.
///
/// Note: please use this way carefully!
///
/// ```no_run
/// use futures::io::Cursor;
/// use opendal::io_util::DecompressReader;
/// use opendal::io_util::CompressAlgorithm;
/// use opendal::io_util::DecompressState;
/// # use std::io::Result;
/// # use futures::AsyncReadExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let content = vec![0; 16 * 1024 * 1024];
/// let mut cr = DecompressReader::new(Cursor::new(content), CompressAlgorithm::Gzip);
/// let mut result = vec![0; 16 * 1024 * 1024];
/// let mut buf = Vec::new();
/// let mut cnt = 0;
/// loop {
///     let (_, output) = result.split_at_mut(cnt);
///
///     match cr.state() {
///         DecompressState::Reading => {
///             buf = cr.fetch().await?.to_vec();
///         }
///         DecompressState::Decoding => {
///             let written = cr.decode(&buf, output)?;
///             cnt += written;
///         }
///         DecompressState::Finishing => {
///             let written = cr.finish(output)?;
///             cnt += written;
///         }
///         DecompressState::Done => {
///             break;
///         }
///     }
/// }
/// # Ok(())
/// }
/// ```
#[derive(Debug)]
#[pin_project]
pub struct DecompressReader<R: BytesRead> {
    #[pin]
    reader: BufReader<R>,
    decoder: DecompressDecoder,
    state: DecompressState,
    multiple_members: bool,
}

impl<R: BytesRead> DecompressReader<R> {
    /// Create a new DecompressReader.
    pub fn new(reader: R, algo: CompressAlgorithm) -> Self {
        Self {
            reader: BufReader::new(reader),
            decoder: algo.into(),
            state: DecompressState::Reading,
            multiple_members: false,
        }
    }

    /// Get decompress state
    pub fn state(&self) -> DecompressState {
        self.state
    }

    /// Fetch more data from underlying reader.
    pub async fn fetch(&mut self) -> Result<&[u8]> {
        debug_assert_eq!(self.state, DecompressState::Reading);

        let buf = self.reader.fill_buf().await?;
        self.state = DecompressState::Decoding;
        Ok(buf)
    }

    /// Decode data into output.
    /// Returns the data that has been written.
    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize> {
        debug_assert_eq!(self.state, DecompressState::Decoding);

        // If input is empty, inner reader must reach EOF, return directly.
        if input.is_empty() {
            debug!("input is empty, return directly");
            // Avoid attempting to reinitialise the decoder if the reader
            // has returned EOF.
            self.multiple_members = false;
            self.state = DecompressState::Finishing;
            return Ok(0);
        }

        let mut input = PartialBuffer::new(input);
        let mut output = PartialBuffer::new(output);
        let done = self.decoder.decode(&mut input, &mut output)?;
        let len = input.written().len();
        debug!("advance reader with amt {}", len);
        Pin::new(&mut self.reader).consume(len);

        if done {
            self.state = DecompressState::Finishing;
        } else {
            self.state = DecompressState::Reading;
        }

        Ok(output.written().len())
    }

    /// Finish a decompress press, flushing remaining data into output.
    /// Return the data that has been written.
    pub fn finish(&mut self, output: &mut [u8]) -> Result<usize> {
        debug_assert_eq!(self.state, DecompressState::Finishing);

        let mut output = PartialBuffer::new(output);
        let done = self.decoder.finish(&mut output)?;
        if done {
            if self.multiple_members {
                self.decoder.reinit()?;
                self.state = DecompressState::Reading;
            } else {
                self.state = DecompressState::Done;
            }
        } else {
            self.state = DecompressState::Finishing;
        }

        Ok(output.written().len())
    }
}

impl<R: BytesRead> futures::io::AsyncRead for DecompressReader<R> {
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
            if !output.written().is_empty() {
                return Poll::Ready(Ok(output.written().len()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Result;

    use async_compression::futures::bufread::GzipEncoder;
    use async_compression::futures::bufread::ZlibEncoder;
    use futures::io::Cursor;
    use futures::AsyncReadExt;
    use rand::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_decompress_decode_zlib() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut e = ZlibEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let br = BufReader::new(Cursor::new(compressed_content));
        let mut cr = DecompressReader::new(br, CompressAlgorithm::Zlib);

        let mut result = vec![0; size];
        let mut cnt = 0;
        let mut buf = Vec::new();
        loop {
            let (_, output) = result.split_at_mut(cnt);

            match cr.state {
                DecompressState::Reading => {
                    buf = cr.fetch().await?.to_vec();
                }
                DecompressState::Decoding => {
                    let written = cr.decode(&buf, output)?;
                    cnt += written;
                }
                DecompressState::Finishing => {
                    let written = cr.finish(output)?;
                    cnt += written;
                }
                DecompressState::Done => {
                    break;
                }
            }
        }

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_zlib() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);

        let mut e = ZlibEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Zlib);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_decode_gzip() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        let mut e = GzipEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let br = BufReader::new(Cursor::new(compressed_content));
        let mut cr = DecompressReader::new(br, CompressAlgorithm::Gzip);

        let mut result = vec![0; size];
        let mut cnt = 0;
        let mut buf = Vec::new();
        loop {
            let (_, output) = result.split_at_mut(cnt);

            match cr.state {
                DecompressState::Reading => {
                    buf = cr.fetch().await?.to_vec();
                }
                DecompressState::Decoding => {
                    let written = cr.decode(&buf, output)?;
                    cnt += written;
                }
                DecompressState::Finishing => {
                    let written = cr.finish(output)?;
                    cnt += written;
                }
                DecompressState::Done => {
                    break;
                }
            }
        }

        assert_eq!(result, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_decompress_reader_gzip() -> Result<()> {
        let _ = env_logger::try_init();

        let mut rng = ThreadRng::default();
        let mut content = vec![0; 16 * 1024 * 1024];
        rng.fill_bytes(&mut content);

        let mut e = GzipEncoder::new(Cursor::new(content.clone()));
        let mut compressed_content = vec![];
        e.read_to_end(&mut compressed_content).await?;

        let mut cr =
            DecompressReader::new(Cursor::new(compressed_content), CompressAlgorithm::Gzip);

        let mut result = vec![];
        cr.read_to_end(&mut result).await?;

        assert_eq!(result, content);

        Ok(())
    }
}
