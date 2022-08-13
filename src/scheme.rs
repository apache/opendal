use std::fmt::Display;
use std::fmt::Formatter;
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
use std::io;
use std::str::FromStr;

use anyhow::anyhow;

use crate::error::other;
use crate::error::BackendError;

/// Backends that OpenDAL supports
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Scheme {
    /// [azblob][crate::services::azblob]: Azure Storage Blob services.
    Azblob,
    /// [fs][crate::services::fs]: POSIX alike file system.
    Fs,
    /// [hdfs][crate::services::hdfs]: Hadoop Distributed File System.
    #[cfg(feature = "services-hdfs")]
    Hdfs,
    /// [http][crate::services::http]: HTTP backend.
    #[cfg(feature = "services-http")]
    Http,
    /// [memory][crate::services::memory]: In memory backend support.
    Memory,
    /// [s3][crate::services::s3]: AWS S3 alike services.
    S3,
}

impl Scheme {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Default for Scheme {
    fn default() -> Self {
        Self::Memory
    }
}

impl Display for Scheme {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Azblob => write!(f, "azblob"),
            Scheme::Fs => write!(f, "fs"),
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => write!(f, "hdfs"),
            #[cfg(feature = "services-http")]
            Scheme::Http => write!(f, "http"),
            Scheme::Memory => write!(f, "memory"),
            Scheme::S3 => write!(f, "s3"),
        }
    }
}

impl FromStr for Scheme {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "fs" => Ok(Scheme::Fs),
            #[cfg(feature = "services-hdfs")]
            "hdfs" => Ok(Scheme::Hdfs),
            #[cfg(feature = "services-http")]
            "http" | "https" => Ok(Scheme::Http),
            "memory" => Ok(Scheme::Memory),
            "s3" => Ok(Scheme::S3),
            v => Err(other(BackendError::new(
                Default::default(),
                anyhow!("{} is not supported", v),
            ))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Fs => "fs",
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => "hdfs",
            #[cfg(feature = "services-http")]
            Scheme::Http => "http",
            Scheme::Memory => "memory",
            Scheme::S3 => "s3",
        }
    }
}
