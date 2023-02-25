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

use std::fmt::Display;
use std::fmt::Formatter;

/// ObjectMode represents the corresponding object's mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ObjectMode {
    /// FILE means the object has data to read.
    FILE,
    /// DIR means the object can be listed.
    DIR,
    /// Unknown means we don't know what we can do on this object.
    Unknown,
}

impl ObjectMode {
    /// Check if this object mode is FILE.
    pub fn is_file(self) -> bool {
        self == ObjectMode::FILE
    }
    /// Check if this object mode is DIR.
    pub fn is_dir(self) -> bool {
        self == ObjectMode::DIR
    }
}

impl Default for ObjectMode {
    fn default() -> Self {
        Self::Unknown
    }
}

impl Display for ObjectMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectMode::FILE => write!(f, "file"),
            ObjectMode::DIR => write!(f, "dir"),
            ObjectMode::Unknown => write!(f, "unknown"),
        }
    }
}
