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

use anyhow::anyhow;

/// Key is the key for different key that we used to implement OpenDAL
/// service upon kv service.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Key {
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
        /// Seperator to make the entry.
        sep: u8,
        /// Name of this index key.
        name: String,
    },
    /// Version key represents a version of inode's blocks.
    ///
    /// We use this key to make sure every write to an inode is atomic.
    Version(u64),
}

impl Key {
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
            sep: b':',
            name: name.to_string(),
        }
    }

    /// Create the prefix for specified prefix.
    ///
    /// # Hack
    ///
    /// bincode will encode `String` as `Vec<u8>`. For an emtpy vec, bincode will
    /// add its length: `0`. To represent build a prefix of parent, we need to
    /// remove the last elements.
    pub fn entry_prefix(parent: u64) -> Vec<u8> {
        let bs = Self::entry(parent, "").encode();
        bs[..bs.len() - 1].to_vec()
    }

    /// Convert scoped key into entry (parent, name)
    pub fn into_entry(self) -> (u64, String) {
        if let Key::Entry { parent, name, .. } = self {
            (parent, name)
        } else {
            unreachable!("scope key is not a valid entry: {:?}", self)
        }
    }

    /// Create a new version scope key.
    pub fn version(ino: u64) -> Self {
        Self::Version(ino)
    }

    /// Encode into bytes.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(&self, bincode::config::standard().with_big_endian())
            .expect("must be valid")
    }

    /// Decode from bytes.
    pub fn decode(key: &[u8]) -> Result<Self> {
        let (v, _) =
            bincode::serde::decode_from_slice(key, bincode::config::standard().with_big_endian())
                .map_err(|err| Error::new(ErrorKind::Other, anyhow!("invalid key: {err:?}")))?;

        Ok(v)
    }
}

/// next_prefix will calculate next prefix by adding 1 for the last element.
pub fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut next = prefix.to_vec();

    let mut overflow = true;
    for i in next.iter_mut().rev() {
        if !overflow {
            break;
        }

        if *i == u8::MAX {
            *i = 0;
        } else {
            *i += 1;
            overflow = false;
        }
    }

    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_prefix() {
        let cases = vec![
            ("normal", vec![0, 1], vec![0, 2]),
            ("overflow", vec![0, 1, 2, 3, u8::MAX], vec![0, 1, 2, 4, 0]),
            (
                "overflow twice",
                vec![0, 1, 2, u8::MAX, u8::MAX],
                vec![0, 1, 3, 0, 0],
            ),
        ];

        for (name, input, expected) in cases {
            let actual = next_prefix(&input);

            assert_eq!(expected, actual, "{}", name)
        }
    }
}
