// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Display;
use std::fmt::Formatter;

/// Operation is the name for APIs in `Accessor`.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Default)]
#[non_exhaustive]
pub enum Operation {
    /// Operation for [`crate::raw::Access::info`]
    #[default]
    Info,
    /// Operation for [`crate::raw::Access::create_dir`]
    CreateDir,
    /// Operation for [`crate::raw::Access::read`]
    Read,
    /// Operation for [`crate::raw::oio::Read::read`]
    ReaderRead,
    /// Operation for [`crate::raw::Access::write`]
    Write,
    /// Operation for [`crate::raw::oio::Write::write`]
    WriterWrite,
    /// Operation for [`crate::raw::oio::Write::close`]
    WriterClose,
    /// Operation for [`crate::raw::oio::Write::abort`]
    WriterAbort,
    /// Operation for [`crate::raw::Access::copy`]
    Copy,
    /// Operation for [`crate::raw::Access::rename`]
    Rename,
    /// Operation for [`crate::raw::Access::stat`]
    Stat,
    /// Operation for [`crate::raw::Access::delete`]
    Delete,
    /// Operation for [`crate::raw::oio::Delete::delete`]
    DeleterDelete,
    /// Operation for [`crate::raw::oio::Delete::flush`]
    DeleterFlush,
    /// Operation for [`crate::raw::Access::list`]
    List,
    /// Operation for [`crate::raw::oio::List::next`]
    ListerNext,
    /// Operation for [`crate::raw::Access::presign`]
    Presign,
    /// Operation for [`crate::raw::Access::blocking_create_dir`]
    BlockingCreateDir,
    /// Operation for [`crate::raw::Access::blocking_read`]
    BlockingRead,
    /// Operation for [`crate::raw::oio::BlockingRead::read`]
    BlockingReaderRead,
    /// Operation for [`crate::raw::Access::blocking_write`]
    BlockingWrite,
    /// Operation for [`crate::raw::oio::BlockingWrite::write`]
    BlockingWriterWrite,
    /// Operation for [`crate::raw::oio::BlockingWrite::close`]
    BlockingWriterClose,
    /// Operation for [`crate::raw::Access::blocking_copy`]
    BlockingCopy,
    /// Operation for [`crate::raw::Access::blocking_rename`]
    BlockingRename,
    /// Operation for [`crate::raw::Access::blocking_stat`]
    BlockingStat,
    /// Operation for [`crate::raw::Access::blocking_delete`]
    BlockingDelete,
    /// Operation for [`crate::raw::oio::BlockingDelete::delete`]
    BlockingDeleterDelete,
    /// Operation for [`crate::raw::oio::BlockingDelete::flush`]
    BlockingDeleterFlush,
    /// Operation for [`crate::raw::Access::blocking_list`]
    BlockingList,
    /// Operation for [`crate::raw::oio::BlockingList::next`]
    BlockingListerNext,
}

impl Operation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }

    /// Check if given operation is oneshot or not.
    ///
    /// For example, `Stat` is oneshot but `ReaderRead` could happen multiple times.
    ///
    /// This function can be used to decide take actions based on operations like logging.
    pub fn is_oneshot(&self) -> bool {
        !matches!(
            self,
            Operation::ReaderRead
                | Operation::WriterWrite
                | Operation::ListerNext
                | Operation::DeleterDelete
                | Operation::BlockingReaderRead
                | Operation::BlockingWriterWrite
                | Operation::BlockingListerNext
                | Operation::BlockingDeleterDelete
        )
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<Operation> for &'static str {
    fn from(v: Operation) -> &'static str {
        match v {
            Operation::Info => "metadata",
            Operation::CreateDir => "create_dir",
            Operation::Read => "read",
            Operation::ReaderRead => "Reader::read",
            Operation::Write => "write",
            Operation::WriterWrite => "Writer::write",
            Operation::WriterClose => "Writer::close",
            Operation::WriterAbort => "Writer::abort",
            Operation::Copy => "copy",
            Operation::Rename => "rename",
            Operation::Stat => "stat",
            Operation::Delete => "delete",
            Operation::List => "list",
            Operation::ListerNext => "List::next",
            Operation::Presign => "presign",
            Operation::Batch => "batch",
            Operation::BlockingCreateDir => "blocking_create_dir",
            Operation::BlockingRead => "blocking_read",
            Operation::BlockingReaderRead => "BlockingReader::read",
            Operation::BlockingWrite => "blocking_write",
            Operation::BlockingWriterWrite => "BlockingWriter::write",
            Operation::BlockingWriterClose => "BlockingWriter::close",
            Operation::BlockingCopy => "blocking_copy",
            Operation::BlockingRename => "blocking_rename",
            Operation::BlockingStat => "blocking_stat",
            Operation::BlockingDelete => "blocking_delete",
            Operation::BlockingList => "blocking_list",
            Operation::BlockingListerNext => "BlockingLister::next",
            Operation::DeleterDelete => "Deleter::delete",
            Operation::DeleterFlush => "Deleter::flush",
            Operation::BlockingDeleterDelete => "BlockingDeleter::delete",
            Operation::BlockingDeleterFlush => "BlockingDeleter::flush",
        }
    }
}

impl From<Operation> for String {
    fn from(v: Operation) -> Self {
        v.into_static().to_string()
    }
}
