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

use async_compression::futures::bufread::BrotliDecoder;
use async_compression::futures::bufread::BzDecoder;
use async_compression::futures::bufread::DeflateDecoder;
use async_compression::futures::bufread::GzipDecoder;
use async_compression::futures::bufread::LzmaDecoder;
use async_compression::futures::bufread::XzDecoder;
use async_compression::futures::bufread::ZlibDecoder;
use async_compression::futures::bufread::ZstdDecoder;
use async_compression::futures::write::BrotliEncoder;
use async_compression::futures::write::BzEncoder;
use async_compression::futures::write::DeflateEncoder;
use async_compression::futures::write::GzipEncoder;
use async_compression::futures::write::LzmaEncoder;
use async_compression::futures::write::XzEncoder;
use async_compression::futures::write::ZlibEncoder;
use async_compression::futures::write::ZstdEncoder;
use futures::io::BufReader;
use std::path::PathBuf;

use crate::BytesRead;
use crate::BytesWrite;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum CompressAlgorithm {
    Brotli,
    Bz2,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

impl CompressAlgorithm {
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

    pub fn from_path(path: &str) -> Option<CompressAlgorithm> {
        let ext = PathBuf::from(path)
            .extension()
            .map(|s| s.to_string_lossy())?;

        CompressAlgorithm::from_extension(&ext)
    }

    pub fn into_reader<R: BytesRead>(self, r: R) -> impl BytesRead {
        match self {
            CompressAlgorithm::Brotli => into_brotli_reader(r),
            CompressAlgorithm::Bz2 => into_bz2_reader(r),
            CompressAlgorithm::Deflate => into_deflate_reader(r),
            CompressAlgorithm::Gzip => into_gzip_reader(r),
            CompressAlgorithm::Lzma => into_lzma_reader(r),
            CompressAlgorithm::Xz => into_xz_reader(r),
            CompressAlgorithm::Zlib => into_zlib_reader(r),
            CompressAlgorithm::Zstd => into_zstd_reader(r),
        }
    }

    pub fn into_writer<W: BytesWrite>(self, w: W) -> impl BytesWrite {
        match self {
            CompressAlgorithm::Brotli => into_brotli_writer(w),
            CompressAlgorithm::Bz2 => into_bz2_writer(w),
            CompressAlgorithm::Deflate => into_deflate_writer(w),
            CompressAlgorithm::Gzip => into_gzip_writer(w),
            CompressAlgorithm::Lzma => into_lzma_writer(w),
            CompressAlgorithm::Xz => into_xz_writer(w),
            CompressAlgorithm::Zlib => into_zlib_writer(w),
            CompressAlgorithm::Zstd => into_zstd_writer(w),
        }
    }
}

pub fn into_brotli_reader<R: BytesRead>(r: R) -> BrotliDecoder<BufReader<R>> {
    BrotliDecoder::new(BufReader::new(r))
}

pub fn into_brotli_writer<W: BytesWrite>(w: W) -> BrotliEncoder<W> {
    BrotliEncoder::new(w)
}

pub fn into_bz2_reader<R: BytesRead>(r: R) -> BzDecoder<BufReader<R>> {
    BzDecoder::new(BufReader::new(r))
}

pub fn into_bz2_writer<W: BytesWrite>(w: W) -> BzEncoder<W> {
    BzEncoder::new(w)
}

pub fn into_deflate_reader<R: BytesRead>(r: R) -> DeflateDecoder<BufReader<R>> {
    DeflateDecoder::new(BufReader::new(r))
}

pub fn into_deflate_writer<W: BytesWrite>(w: W) -> DeflateEncoder<W> {
    DeflateEncoder::new(w)
}

pub fn into_gzip_reader<R: BytesRead>(r: R) -> GzipDecoder<BufReader<R>> {
    GzipDecoder::new(BufReader::new(r))
}

pub fn into_gzip_writer<W: BytesWrite>(w: W) -> GzipEncoder<W> {
    GzipEncoder::new(w)
}

pub fn into_lzma_reader<R: BytesRead>(r: R) -> LzmaDecoder<BufReader<R>> {
    LzmaDecoder::new(BufReader::new(r))
}

pub fn into_lzma_writer<W: BytesWrite>(w: W) -> LzmaEncoder<W> {
    LzmaEncoder::new(w)
}

pub fn into_xz_reader<R: BytesRead>(r: R) -> XzDecoder<BufReader<R>> {
    XzDecoder::new(BufReader::new(r))
}

pub fn into_xz_writer<W: BytesWrite>(w: W) -> XzEncoder<W> {
    XzEncoder::new(w)
}

pub fn into_zlib_reader<R: BytesRead>(r: R) -> ZlibDecoder<BufReader<R>> {
    ZlibDecoder::new(BufReader::new(r))
}

pub fn into_zlib_writer<W: BytesWrite>(w: W) -> ZlibEncoder<W> {
    ZlibEncoder::new(w)
}

pub fn into_zstd_reader<R: BytesRead>(r: R) -> ZstdDecoder<BufReader<R>> {
    ZstdDecoder::new(BufReader::new(r))
}

pub fn into_zstd_writer<W: BytesWrite>(w: W) -> ZstdEncoder<W> {
    ZstdEncoder::new(w)
}
