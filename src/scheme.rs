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

use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::str::FromStr;

/// Services that OpenDAL supports
///
/// # Notes
///
/// - Scheme is `non_exhaustive`, new variant COULD be added at any time.
/// - New variant SHOULD be added in alphabet orders,
/// - Users MUST NOT relay on its order.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Scheme {
    /// [azblob][crate::services::azblob]: Azure Storage Blob services.
    Azblob,
    /// [fs][crate::services::fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::gcs]: Google Cloud Storage backend.
    Gcs,
    /// [hdfs][crate::services::hdfs]: Hadoop Distributed File System.
    #[cfg(feature = "services-hdfs")]
    Hdfs,
    /// [http][crate::services::http]: HTTP backend.
    #[cfg(feature = "services-http")]
    Http,
    /// [ftp][crate::services::ftp]: FTP backend.
    #[cfg(feature = "services-ftp")]
    Ftp,
    /// [ipfs][crate::services::ipfs]: IPFS mutable file system
    Ipfs,
    /// [memory][crate::services::memory]: In memory backend support.
    Memory,
    /// [obs][crate::services::obs]: Huawei Cloud OBS services.
    Obs,
    /// [s3][crate::services::s3]: AWS S3 alike services.
    S3,
    /// Custom that allow users to implement services outside of OpenDAL.
    ///
    /// # NOTE
    ///
    /// - Custom must not overwrite any existing services name.
    /// - Custom must be lowed cases.
    Custom(&'static str),
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
            Scheme::Gcs => write!(f, "gcs"),
            #[cfg(feature = "services-http")]
            Scheme::Http => write!(f, "http"),
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => write!(f, "ftp"),
            Scheme::Ipfs => write!(f, "ipfs"),
            Scheme::Memory => write!(f, "memory"),
            Scheme::Obs => write!(f, "obs"),
            Scheme::S3 => write!(f, "s3"),
            Scheme::Custom(v) => write!(f, "{v}"),
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
            "gcs" => Ok(Scheme::Gcs),
            #[cfg(feature = "services-hdfs")]
            "hdfs" => Ok(Scheme::Hdfs),
            #[cfg(feature = "services-http")]
            "http" | "https" => Ok(Scheme::Http),
            #[cfg(feature = "services-ftp")]
            "ftp" => Ok(Scheme::Ftp),
            "ipfs" => Ok(Scheme::Ipfs),
            "memory" => Ok(Scheme::Memory),
            "obs" => Ok(Scheme::Obs),
            "s3" => Ok(Scheme::S3),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Fs => "fs",
            Scheme::Gcs => "gcs",
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => "hdfs",
            #[cfg(feature = "services-http")]
            Scheme::Http => "http",
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => "ftp",
            Scheme::Ipfs => "ipfs",
            Scheme::Memory => "memory",
            Scheme::Obs => "obs",
            Scheme::S3 => "s3",
            Scheme::Custom(v) => v,
        }
    }
}
