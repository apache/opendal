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

#[derive(Debug, Clone, Default)]
pub struct OpRead {
    pub path: String,
    pub offset: Option<u64>,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct OpStat {
    pub path: String,
}

impl OpStat {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpWrite {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct OpDelete {
    pub path: String,
}

impl OpDelete {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpList {
    pub path: String,
}

impl OpList {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeaderRange(Option<u64>, Option<u64>);

impl HeaderRange {
    pub fn new(offset: Option<u64>, size: Option<u64>) -> Self {
        HeaderRange(offset, size)
    }
}

impl ToString for HeaderRange {
    // # NOTE
    //
    // - `bytes=-1023` means get the suffix of the file, we must set the start to 0.
    // - `bytes=0-1023` means get the first 1024 bytes, we must set the end to 1023.
    fn to_string(&self) -> String {
        match (self.0, self.1) {
            (Some(offset), None) => format!("bytes={}-", offset),
            (None, Some(size)) => format!("bytes=0-{}", size - 1),
            (Some(offset), Some(size)) => format!("bytes={}-{}", offset, offset + size - 1),
            _ => panic!("invalid range"),
        }
    }
}
