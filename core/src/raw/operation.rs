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

/// Operation is the name of the operation that is being performed.
///
/// Most operations can be mapped to the methods of the `Access` trait,
/// but we modify the names to make them more readable and clear for users.
///
/// The same operation might have different meanings and costs in different
/// storage services.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Default)]
#[non_exhaustive]
pub enum Operation {
    /// Operation to retrieve information about the specified storage services.
    #[default]
    Info,
    /// Operation to create a directory.
    CreateDir,
    /// Operation to start reading from a file.
    ///
    /// This operation only start to read from the file, it does not read any data.
    ///
    /// For example:
    ///
    /// - On fs alike storage, this operation is used to open a file.
    /// - On s3 alike storage, this operation is used to send a request but not consume it's body.
    ReaderStart,
    /// Operation to perform a single read I/O operation on a file.
    ///
    /// Reader might have to call this operation multiple times to read the whole file.
    ReaderRead,
    /// Operation to start writing to a file.
    ///
    /// This operation only start to write into the file, it does not write any data.
    WriterStart,
    /// Operation to perform a single write I/O operation on a file.
    ///
    /// Writer might have to call this operation multiple times to write the whole file.
    WriterWrite,
    /// Operation to close the writer and finish writing.
    WriterClose,
    /// Operation to abort the writer and discard the data.
    WriterAbort,
    /// Operation to copy a file.
    Copy,
    /// Operation to rename a file.
    Rename,
    /// Operation to stat a file or a directory.
    Stat,
    /// Operation to start deleting files.
    DeleterStart,
    /// Operation to push a delete operation into the queue.
    DeleterDelete,
    /// Operation to flush the deleter queue and perform real delete operations.
    DeleterFlush,
    /// Operation to start listing files.
    ListerStart,
    /// Operation to get the next file from the list.
    ListerNext,
    /// Operation to generate a presigned URL.
    Presign,
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
            Operation::Info => "info",
            Operation::CreateDir => "create_dir",
            Operation::ReaderStart => "reader::start",
            Operation::ReaderRead => "reader::read",
            Operation::WriterStart => "writer::start",
            Operation::WriterWrite => "writer::write",
            Operation::WriterClose => "writer::close",
            Operation::WriterAbort => "writer::abort",
            Operation::Copy => "copy",
            Operation::Rename => "rename",
            Operation::Stat => "stat",
            Operation::DeleterStart => "deleter::start",
            Operation::DeleterDelete => "deleter::delete",
            Operation::DeleterFlush => "deleter::flush",
            Operation::ListerStart => "lister::start",
            Operation::ListerNext => "lister::next",
            Operation::Presign => "presign",
        }
    }
}

impl From<Operation> for String {
    fn from(v: Operation) -> Self {
        v.into_static().to_string()
    }
}
