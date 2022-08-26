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

/// Operation is the name for APIs in `Accessor`.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum Operation {
    /// Operation for [`crate::Accessor::metadata`]
    Metadata,
    /// Operation for [`crate::Accessor::create`]
    Create,
    /// Operation for [`crate::Accessor::read`]
    Read,
    /// Operation for [`crate::Accessor::write`]
    Write,
    /// Operation for [`crate::Accessor::stat`]
    Stat,
    /// Operation for [`crate::Accessor::delete`]
    Delete,
    /// Operation for [`crate::Accessor::list`]
    List,
    /// Operation for [`crate::Accessor::presign`]
    Presign,
    /// Operation for [`crate::Accessor::create_multipart`]
    CreateMultipart,
    /// Operation for [`crate::Accessor::write_multipart`]
    WriteMultipart,
    /// Operation for [`crate::Accessor::complete_multipart`]
    CompleteMultipart,
    /// Operation for [`crate::Accessor::abort_multipart`]
    AbortMultipart,
}

impl Operation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Default for Operation {
    fn default() -> Self {
        Operation::Metadata
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Metadata => write!(f, "metadata"),
            Operation::Create => write!(f, "create"),
            Operation::Read => write!(f, "read"),
            Operation::Write => write!(f, "write"),
            Operation::Stat => write!(f, "stat"),
            Operation::Delete => write!(f, "delete"),
            Operation::List => write!(f, "list"),
            Operation::Presign => write!(f, "presign"),
            Operation::CreateMultipart => write!(f, "create_multipart"),
            Operation::WriteMultipart => write!(f, "write_multipart"),
            Operation::CompleteMultipart => write!(f, "complete_multipart"),
            Operation::AbortMultipart => write!(f, "abort_multipart"),
        }
    }
}

impl From<Operation> for &'static str {
    fn from(v: Operation) -> &'static str {
        match v {
            Operation::Metadata => "metadata",
            Operation::Create => "create",
            Operation::Read => "read",
            Operation::Write => "write",
            Operation::Stat => "stat",
            Operation::Delete => "delete",
            Operation::List => "list",
            Operation::Presign => "presign",
            Operation::CreateMultipart => "create_multipart",
            Operation::WriteMultipart => "write_multipart",
            Operation::CompleteMultipart => "complete_multipart",
            Operation::AbortMultipart => "abort_multipart",
        }
    }
}
