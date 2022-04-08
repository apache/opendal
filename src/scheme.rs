use std::io;
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
use std::str::FromStr;

use anyhow::anyhow;

use crate::error::other;
use crate::error::BackendError;

/// Backends that OpenDAL supports
#[derive(Clone, Debug, PartialEq)]
pub enum Scheme {
    /// [azblob][crate::services::azblob]: Azure Storage Blob services.
    Azblob,
    /// [fs][crate::services::fs]: POSIX alike file system.
    Fs,
    /// [memory][crate::services::memory]: In memory backend support.
    Memory,
    /// [s3][crate::services::s3]: AWS S3 alike services.
    S3,
}

impl FromStr for Scheme {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "fs" => Ok(Scheme::Fs),
            "memory" => Ok(Scheme::Memory),
            "s3" => Ok(Scheme::S3),

            // TODO: it's used for compatibility with dal1, should be removed in the future
            "local" | "disk" => Ok(Scheme::Fs),
            "azurestorageblob" => Ok(Scheme::Azblob),

            v => Err(other(BackendError::new(
                Default::default(),
                anyhow!("{} is not supported", v),
            ))),
        }
    }
}
