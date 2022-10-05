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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::mem::size_of;

use anyhow::anyhow;

/// ScopedKey is the key for different key that we used to implement OpenDAL
/// service upon kv service.
#[derive(Debug)]
pub enum ScopedKey {
    /// Meta key of this scope.
    ///
    /// Every kv services will only have on of this key.
    Meta,
    /// Inode key represents an inode which stores the metadata of a directory
    /// or file.
    Inode(u64),
    /// Block key represents the actual data of a file.
    ///
    /// A file could be spilt into multiple blocks which stores in inode `blocks`.
    Block {
        /// Inode of this block.
        ino: u64,
        /// Version of this block.
        version: u64,
        /// Block id of this block.
        block: u64,
    },
    /// Index key represents an entry under an inode.
    Entry {
        /// Parent of this index key.
        parent: u64,
        /// Name of this index key.
        name: String,
    },
    /// Version key represents a version of inode's blocks.
    ///
    /// We use this key to make sure every write to an inode is atomic.
    Version(u64),
}

impl ScopedKey {
    /// Create a meta scope key.
    pub fn meta() -> Self {
        Self::Meta
    }

    /// Create an inode scope key.
    pub fn inode(ino: u64) -> Self {
        Self::Inode(ino)
    }

    /// Create a block scope key
    pub fn block(ino: u64, version: u64, block: u64) -> Self {
        Self::Block {
            ino,
            version,
            block,
        }
    }

    /// Create a new entry scope key.
    pub fn entry(parent: u64, name: &str) -> Self {
        let name = name.trim_end_matches('/');

        Self::Entry {
            parent,
            name: name.to_string(),
        }
    }

    /// Convert scoped key into entry (parent, name)
    pub fn into_entry(self) -> (u64, String) {
        if let ScopedKey::Entry { parent, name } = self {
            (parent, name)
        } else {
            unreachable!("scope key is not a valid entry: {:?}", self)
        }
    }

    /// Create a new version scope key.
    pub fn version(ino: u64) -> Self {
        Self::Version(ino)
    }

    /// Get the scope of this key
    #[inline]
    fn scope(&self) -> u8 {
        match self {
            ScopedKey::Meta => 0,
            ScopedKey::Inode(_) => 1,
            ScopedKey::Block { .. } => 2,
            ScopedKey::Entry { .. } => 3,
            ScopedKey::Version(_) => 4,
        }
    }

    /// Get the size of this key after encode.
    #[inline]
    pub fn size(&self) -> usize {
        let size_of_u64 = size_of::<u64>();

        1 + match self {
            ScopedKey::Meta => 0,
            ScopedKey::Inode(_) => size_of_u64,
            ScopedKey::Block { .. } => size_of_u64 * 3,
            ScopedKey::Entry { parent: _, name } => size_of_u64 + name.as_bytes().len(),
            ScopedKey::Version(_) => size_of_u64,
        }
    }

    /// Encode into bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.size());
        data.push(self.scope());
        match self {
            ScopedKey::Meta => (),
            ScopedKey::Inode(ino) => data.extend(ino.to_be_bytes()),
            ScopedKey::Block {
                ino,
                version,
                block,
            } => {
                data.extend(ino.to_be_bytes());
                data.extend(version.to_be_bytes());
                data.extend(block.to_be_bytes());
            }
            ScopedKey::Entry { parent, name } => {
                data.extend(parent.to_be_bytes());
                data.extend(name.as_bytes());
            }
            ScopedKey::Version(version) => {
                data.extend(version.to_be_bytes());
            }
        }
        data
    }

    /// Decode from bytes.
    pub fn decode(key: &[u8]) -> Result<Self> {
        let invalid_key = || Error::new(ErrorKind::Other, anyhow!("invalid scope key"));
        let size_of_u64 = size_of::<u64>();

        let (scope, data) = key.split_first().ok_or_else(invalid_key)?;

        match *scope {
            0 => Ok(Self::Meta),
            1 => {
                let ino = u64::from_be_bytes(data.try_into().map_err(|_| invalid_key())?);
                Ok(Self::inode(ino))
            }
            2 => {
                let ino =
                    u64::from_be_bytes(data[..size_of_u64].try_into().map_err(|_| invalid_key())?);
                let version = u64::from_be_bytes(
                    data[size_of_u64..size_of_u64 * 2]
                        .try_into()
                        .map_err(|_| invalid_key())?,
                );
                let block = u64::from_be_bytes(
                    data[size_of_u64 * 2..]
                        .try_into()
                        .map_err(|_| invalid_key())?,
                );
                Ok(Self::block(ino, version, block))
            }
            3 => {
                let parent =
                    u64::from_be_bytes(data[..size_of_u64].try_into().map_err(|_| invalid_key())?);
                let name = std::str::from_utf8(&data[size_of_u64..]).map_err(|_| invalid_key())?;
                Ok(Self::entry(parent, name))
            }
            4 => {
                let ino = u64::from_be_bytes(data.try_into().map_err(|_| invalid_key())?);
                Ok(Self::version(ino))
            }
            _ => Err(invalid_key()),
        }
    }
}
