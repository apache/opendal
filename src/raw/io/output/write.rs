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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use async_trait::async_trait;
use bytes::Bytes;

/// Writer is a type erased [`Write`]
pub type Writer = Box<dyn Write>;

/// Write is the trait that OpenDAL returns to callers.
#[async_trait]
pub trait Write: Unpin + Send + Sync {
    /// If this writer can be used as appendable writer.
    const APPENDABLE: bool;

    /// Write whole content at once.
    ///
    /// We consume the writer here to indicate that users should
    /// write all content. To append multiple bytes together, use
    /// `append` instead.
    async fn write(self, bs: Bytes) -> Result<()>;

    /// Initiate a process of append write.
    ///
    /// After initiate, users can append multiple bytes into this write.
    async fn initiate(&mut self) -> Result<()>;

    /// Append bytes to the writer.
    ///
    /// It is highly recommended to align the length of the input bytes
    /// into blocks of 4MiB (except the last block) for better performance
    /// and compatibility.
    async fn append(&mut self, bs: Bytes) -> Result<()>;

    /// Complete the append process
    async fn complete(self) -> Result<()>;
}

#[async_trait]
impl Write for () {
    const APPENDABLE: bool = false;

    async fn write(self, bs: Bytes) -> Result<()> {
        let _ = bs;

        unimplemented!("write is required to be implemented for output::Write")
    }

    async fn initiate(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support initiate",
        ))
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        let _ = bs;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support append",
        ))
    }

    async fn complete(self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support complete",
        ))
    }
}
