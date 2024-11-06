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

use std::cell::RefCell;
use std::collections::HashSet;
use std::io::BufRead;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use magnus::class;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;

use crate::*;

// `OpenDALIO` follows similar Ruby IO classes, such as:
// - IO
// - StringIO
//
// `OpenDALIO` is not exactly an `IO` but is unidirectional (either `Reader` or `Writer`).
// TODO: implement encoding.
//
// The name of OpenDALIO is an arbitrary choice. Open to changes before 1.0.
#[magnus::wrap(class = "OpenDAL::IO", free_immediately, size)]
pub struct OpenDALIO(RefCell<FileState>);

enum FileState {
    Reader(ocore::StdReader, bool), // bool indicates binary mode
    Writer(ocore::StdWriter, bool), // bool indicates binary mode
    Closed,
}

pub fn format_io_error(err: std::io::Error) -> Error {
    Error::new(exception::runtime_error(), err.to_string())
}

impl OpenDALIO {
    /// Creates a new `OpenDAL::IO` object in Ruby.
    ///
    // @param ruby Ruby handle, required for exception handling.
    // @param operator OpenDAL operator for file operations.
    // @param path Path to the file.
    // @param mode Mode string, e.g., "r", "w", or "rb".
    //
    // The mode must contain unique characters. Invalid or duplicate modes will raise an `ArgumentError`.
    pub fn new(
        ruby: &Ruby,
        operator: ocore::BlockingOperator,
        path: String,
        mode: String,
    ) -> Result<Self, Error> {
        let mut mode_flags = HashSet::new();
        let is_unique = mode.chars().all(|c| mode_flags.insert(c));
        if !is_unique {
            return Err(Error::new(
                ruby.exception_arg_error(),
                format!("Invalid access mode {mode}"),
            ));
        }

        let binary_mode = mode_flags.contains(&'b');

        if mode_flags.contains(&'r') {
            Ok(Self(RefCell::new(FileState::Reader(
                operator
                    .reader(&path)
                    .map_err(format_magnus_error)?
                    .into_std_read(..)
                    .map_err(format_magnus_error)?,
                binary_mode,
            ))))
        } else if mode_flags.contains(&'w') {
            Ok(Self(RefCell::new(FileState::Writer(
                operator
                    .writer(&path)
                    .map_err(format_magnus_error)?
                    .into_std_write(),
                binary_mode,
            ))))
        } else {
            Err(Error::new(
                ruby.exception_runtime_error(),
                format!("OpenDAL doesn't support mode: {mode}"),
            ))
        }
    }

    /// Enables binary mode for the stream.
    fn binary_mode(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let mut cell = rb_self.0.borrow_mut();
        match &mut *cell {
            FileState::Reader(_, ref mut is_binary_mode) => {
                *is_binary_mode = true;
                Ok(())
            }
            FileState::Writer(_, ref mut is_binary_mode) => {
                *is_binary_mode = true;
                Ok(())
            }
            FileState::Closed => Err(Error::new(ruby.exception_io_error(), "closed stream")),
        }
    }

    /// Returns if the stream is on binary mode.
    fn is_binary_mode(ruby: &Ruby, rb_self: &Self) -> Result<bool, Error> {
        match *rb_self.0.borrow() {
            FileState::Reader(_, is_binary_mode) => Ok(is_binary_mode),
            FileState::Writer(_, is_binary_mode) => Ok(is_binary_mode),
            FileState::Closed => Err(Error::new(ruby.exception_io_error(), "closed stream")),
        }
    }

    /// Checks if the stream is in binary mode.
    fn close(&self) -> Result<(), Error> {
        // skips closing reader because `StdReader` doesn't have `close()`.
        let mut cell = self.0.borrow_mut();
        if let FileState::Writer(writer, _) = &mut *cell {
            writer.close().map_err(format_io_error)?;
        }
        *cell = FileState::Closed;
        Ok(())
    }

    /// Closes the stream and transitions the state to `Closed`.
    fn close_read(&self) -> Result<(), Error> {
        *self.0.borrow_mut() = FileState::Closed;
        Ok(())
    }

    /// Reads data from the stream.
    fn close_write(&self) -> Result<(), Error> {
        let mut cell = self.0.borrow_mut();
        if let FileState::Writer(writer, _) = &mut *cell {
            writer.close().map_err(format_io_error)?;
        }
        *cell = FileState::Closed;
        Ok(())
    }

    fn is_closed(&self) -> Result<bool, Error> {
        Ok(matches!(*self.0.borrow(), FileState::Closed))
    }

    fn is_closed_read(&self) -> Result<bool, Error> {
        Ok(!matches!(*self.0.borrow(), FileState::Reader(_, _)))
    }

    fn is_closed_write(&self) -> Result<bool, Error> {
        Ok(!matches!(*self.0.borrow(), FileState::Writer(_, _)))
    }
}

impl OpenDALIO {
    /// Reads data from the stream.
    /// TODO:
    ///   - support default parameters
    ///   - support encoding
    ///
    /// @param size The maximum number of bytes to read. Reads all data if `None`.
    fn read(ruby: &Ruby, rb_self: &Self, size: Option<usize>) -> Result<bytes::Bytes, Error> {
        // FIXME: consider what to return exactly
        if let FileState::Reader(reader, _) = &mut *rb_self.0.borrow_mut() {
            let buffer = match size {
                Some(size) => {
                    let mut bs = vec![0; size];
                    let n = reader.read(&mut bs).map_err(format_io_error)?;
                    bs.truncate(n);
                    bs
                }
                None => {
                    let mut buffer = Vec::new();
                    reader.read_to_end(&mut buffer).map_err(format_io_error)?;
                    buffer
                }
            };

            Ok(buffer.into())
        } else {
            Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on closed file.",
            ))
        }
    }

    /// Reads a single line from the stream.
    // TODO: extend readline with parameters
    fn readline(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        if let FileState::Reader(reader, _) = &mut *rb_self.0.borrow_mut() {
            let mut buffer = String::new();
            let size = reader.read_line(&mut buffer).map_err(format_io_error)?;
            if size == 0 {
                return Err(Error::new(
                    ruby.exception_eof_error(),
                    "end of file reached",
                ));
            }

            Ok(buffer)
        } else {
            Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on closed file.",
            ))
        }
    }

    /// Writes data to the stream.
    ///
    /// @param bs The string data to write to the stream.
    fn write(ruby: &Ruby, rb_self: &Self, bs: String) -> Result<usize, Error> {
        if let FileState::Writer(writer, _) = &mut *rb_self.0.borrow_mut() {
            Ok(writer
                .write_all(bs.as_bytes())
                .map(|_| bs.len())
                .map_err(format_io_error)?)
        } else {
            Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on write only file.",
            ))
        }
    }
}

impl OpenDALIO {
    /// Moves the file position based on the offset and whence.
    ///
    /// @param offset The position offset.
    /// @param whence The reference point:
    ///   - 0 = IO:SEEK_SET (Start)
    ///   - 1 = IO:SEEK_CUR (Current position)
    ///   - 2 = IO:SEEK_END (From the end)
    fn seek(ruby: &Ruby, rb_self: &Self, offset: i64, whence: u8) -> Result<u8, Error> {
        match &mut *rb_self.0.borrow_mut() {
            FileState::Reader(reader, _) => {
                let whence = match whence {
                    0 => SeekFrom::Start(offset as u64),
                    1 => SeekFrom::Current(offset),
                    2 => SeekFrom::End(offset),
                    _ => return Err(Error::new(ruby.exception_arg_error(), "invalid whence")),
                };

                reader.seek(whence).map_err(format_io_error)?;

                Ok(0)
            }
            FileState::Writer(_, _) => Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on write only file.",
            )),
            FileState::Closed => Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on closed file.",
            )),
        }
    }

    /// Returns the current position of the file pointer in the stream.
    fn tell(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        match &mut *rb_self.0.borrow_mut() {
            FileState::Reader(reader, _) => {
                Ok(reader.stream_position().map_err(format_io_error)?)
            }
            FileState::Writer(_, _) => Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on write only file.",
            )),
            FileState::Closed => Err(Error::new(
                ruby.exception_runtime_error(),
                "I/O operation failed for reading on closed file.",
            )),
        }
    }

    // TODO: consider implement:
    // - lineno
    // - set_lineno
    // - getc
    // - putc
    // - gets
    // - puts
}

/// Defines the `OpenDAL::IO` class in the given Ruby module and binds its methods.
///
/// This function uses Magnus's built-in Ruby thread-safety features to define the
/// `OpenDAL::IO` class and its methods in the provided Ruby module (`gem_module`).
///
/// # Ruby Object Lifetime and Safety
///
/// Ruby objects can only exist in the Ruby heap and are tracked by Ruby's garbage collector (GC).
/// While we can allocate and store Ruby-related objects in the Rust heap, Magnus does not
/// automatically track such objects. Therefore, it is critical to work within Magnus's safety
/// guidelines when integrating Rust objects with Ruby. Read more in the Magnus documentation:
/// [Magnus Safety Documentation](https://github.com/matsadler/magnus#safety).
pub fn include(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("IO", class::object())?;
    class.define_method("binmode", method!(OpenDALIO::binary_mode, 0))?;
    class.define_method("binmode?", method!(OpenDALIO::is_binary_mode, 0))?;
    class.define_method("close", method!(OpenDALIO::close, 0))?;
    class.define_method("close_read", method!(OpenDALIO::close_read, 0))?;
    class.define_method("close_write", method!(OpenDALIO::close_write, 0))?;
    class.define_method("closed?", method!(OpenDALIO::is_closed, 0))?;
    class.define_method("closed_read?", method!(OpenDALIO::is_closed_read, 0))?;
    class.define_method("closed_write?", method!(OpenDALIO::is_closed_write, 0))?;
    class.define_method("read", method!(OpenDALIO::read, 1))?;
    class.define_method("write", method!(OpenDALIO::write, 1))?;
    class.define_method("readline", method!(OpenDALIO::readline, 0))?;
    class.define_method("seek", method!(OpenDALIO::seek, 2))?;
    class.define_method("tell", method!(OpenDALIO::tell, 0))?;

    Ok(())
}
