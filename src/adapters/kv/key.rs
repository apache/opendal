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
#[derive(Debug, Eq, PartialEq)]
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
}

impl Key {
    /// Create an inode scope key.
    pub fn inode(ino: u64) -> Self {
        Self::Inode(ino)
    }

    /// Create a block scope key
    pub fn block(ino: u64, block: u64) -> Self {
        Self::Block { ino, block }
    }

    /// Create a new entry scope key.
    pub fn entry(parent: u64, name: &str) -> Self {
        let name = name.trim_end_matches('/');

        Self::Entry {
            parent,
            name: name.to_string(),
        }
    }

    /// Create the prefix for specified inode's blocks.
    pub fn block_prefix(ino: u64) -> Vec<u8> {
        format!("b:{ino}:").into_bytes()
    }

    /// Create the prefix for specified prefix.
    pub fn entry_prefix(parent: u64) -> Vec<u8> {
        format!("e:{parent}:").into_bytes()
    }

    /// Convert scoped key into entry (parent, name)
    pub fn into_entry(self) -> (u64, String) {
        if let Key::Entry { parent, name } = self {
            (parent, name)
        } else {
            unreachable!("key is not a valid entry: {:?}", self)
        }
    }

    /// Encode into bytes.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Key::Inode(v) => format!("i:{v}").into_bytes(),
            Key::Block { ino, block } => format!("b:{ino}:{block}").into_bytes(),
            Key::Entry { parent, name } => format!("e:{parent}:{name}").into_bytes(),
        }
    }

    /// Decode from bytes.
    pub fn decode(key: &[u8]) -> Result<Self> {
        let s = String::from_utf8(key.to_vec()).expect("must be valid string");
        let (typ, s) = s.split_at(2);
        match typ.as_bytes()[0] {
            b'i' => {
                let v = s.parse().expect("must be valid u64");
                Ok(Key::inode(v))
            }
            b'b' => {
                let idx = s.find(':').expect("must have valid /");
                let ino = s[..idx].parse().expect("must be valid u64");
                let block = s[idx + 1..].parse().expect("must be valid u64");
                Ok(Key::block(ino, block))
            }
            b'e' => {
                let idx = s.find(':').expect("must have valid /");
                let parent = s[..idx].parse().expect("must be valid u64");
                let name = &s[idx + 1..];
                Ok(Key::entry(parent, name))
            }
            _ => Err(Error::new(ErrorKind::Other, anyhow!("invalid key"))),
        }
    }
}

/// next_prefix will calculate next prefix by adding 1 for the last element.
///
/// The last byte of prefix is always `:`, it's ok to `+=1`
///
/// # Notes
///
/// This function should only be used in implementing `Adapter::scan` to calculate
/// the correct range for keys.
pub fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    debug_assert!(
        prefix.last() == Some(&b':'),
        "input prefix must end with ':'"
    );

    let mut next = prefix.to_vec();
    *next.last_mut().unwrap() += 1;
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode() {
        let cases = vec![
            ("inode", "i:123", Key::inode(123)),
            ("block", "b:123:456", Key::block(123, 456)),
            ("entry", "e:123:中文测试", Key::entry(123, "中文测试")),
        ];
        for (name, input, expect) in cases {
            let actual = Key::decode(input.as_bytes()).expect("must be valid");
            assert_eq!(expect, actual, "{}", name)
        }
    }
}
