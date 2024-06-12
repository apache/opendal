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

use std::io::Write;
use std::mem::size_of;

use vm_memory::ByteValued;

use crate::error::*;
use crate::server_message::*;
use crate::virtiofs_utils::{Reader, Writer};

/// Version number of this interface.
const KERNEL_VERSION: u32 = 7;
/// Minor version number of this interface.
const KERNEL_MINOR_VERSION: u32 = 38;
/// Minmum Minor version number supported.
const MIN_KERNEL_MINOR_VERSION: u32 = 27;
/// The length of the header part of the message.
const BUFFER_HEADER_SIZE: u32 = 256;
/// The maximum length of the data part of the message, used for read/write data.
const MAX_BUFFER_SIZE: u32 = 1 << 20;

/// Server is a filesystem implementation, will decode and process messages from VMs.
pub struct Server {}

#[allow(dead_code)]
impl Server {
    pub fn new() -> Server {
        Server {}
    }

    pub fn handle_message(&self, mut r: Reader, w: Writer) -> Result<usize> {
        let in_header: InHeader = r
            .read_obj()
            .map_err(|e| new_vhost_user_fs_error("", Some(e.into())))?;
        if in_header.len > (MAX_BUFFER_SIZE + BUFFER_HEADER_SIZE) {
            // The message is too long here.
            return reply_error(in_header.unique, w);
        }
        if let Ok(opcode) = Opcode::try_from(in_header.opcode) {
            match opcode {
                Opcode::Init => self.init(in_header, r, w),
            }
        } else {
            reply_error(in_header.unique, w)
        }
    }

    fn init(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let InitIn { major, minor, .. } = r.read_obj().map_err(|e| {
            new_vhost_user_fs_error("failed to decode protocol messages", Some(e.into()))
        })?;

        if major != KERNEL_VERSION || minor < MIN_KERNEL_MINOR_VERSION {
            return reply_error(in_header.unique, w);
        }

        // We will directly return ok and do nothing for now.
        let out = InitOut {
            major: KERNEL_VERSION,
            minor: KERNEL_MINOR_VERSION,
            max_write: MAX_BUFFER_SIZE,
            ..Default::default()
        };
        reply_ok(Some(out), None, in_header.unique, w)
    }
}

fn reply_ok<T: ByteValued>(
    out: Option<T>,
    data: Option<&[u8]>,
    unique: u64,
    mut w: Writer,
) -> Result<usize> {
    let mut len = size_of::<OutHeader>();
    if out.is_some() {
        len += size_of::<T>();
    }
    if let Some(data) = data {
        len += data.len();
    }
    let header = OutHeader {
        unique,
        error: 0, // Return no error.
        len: len as u32,
    };
    w.write_all(header.as_slice()).map_err(|e| {
        new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
    })?;
    if let Some(out) = out {
        w.write_all(out.as_slice()).map_err(|e| {
            new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
        })?;
    }
    if let Some(data) = data {
        w.write_all(data).map_err(|e| {
            new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
        })?;
    }
    Ok(w.bytes_written())
}

fn reply_error(unique: u64, mut w: Writer) -> Result<usize> {
    let header = OutHeader {
        unique,
        error: libc::EIO, // Here we simply return I/O error.
        len: size_of::<OutHeader> as u32,
    };
    w.write_all(header.as_slice()).map_err(|e| {
        new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
    })?;
    Ok(w.bytes_written())
}
