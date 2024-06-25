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

use vm_memory::ByteValued;

use crate::error::*;

/// Opcode represents the filesystem call that needs to be executed by VMs message.
/// The corresponding value needs to be aligned with the specification.
#[non_exhaustive]
pub enum Opcode {
    Init = 26,
}

impl TryFrom<u32> for Opcode {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            26 => Ok(Opcode::Init),
            _ => Err(new_vhost_user_fs_error("failed to decode opcode", None)),
        }
    }
}

/// InHeader represents the incoming message header in the filesystem call.
/// 
/// The fields of the struct need to conform to the specific format of the virtiofs message.
/// Currently, we only need to align them exactly with vitrtiofsd.
/// Reference: https://gitlab.com/virtio-fs/virtiofsd/-/blob/main/src/fuse.rs?ref_type=heads#L1155
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InHeader {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub total_extlen: u16,
    pub padding: u16,
}

/// OutHeader represents the message header returned in the filesystem call.
/// 
/// The fields of the struct need to conform to the specific format of the virtiofs message.
/// Currently, we only need to align them exactly with vitrtiofsd.
/// Reference: https://gitlab.com/virtio-fs/virtiofsd/-/blob/main/src/fuse.rs?ref_type=heads#L1170
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct OutHeader {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

/// InitIn is used to parse the parameters passed in the Init filesystem call.
/// 
/// The fields of the struct need to conform to the specific format of the virtiofs message.
/// Currently, we only need to align them exactly with vitrtiofsd.
/// Reference: https://gitlab.com/virtio-fs/virtiofsd/-/blob/main/src/fuse.rs?ref_type=heads#L1030
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InitIn {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

/// InitOut is used to return the result of the Init filesystem call.
/// 
/// The fields of the struct need to conform to the specific format of the virtiofs message.
/// Currently, we only need to align them exactly with vitrtiofsd.
/// Reference: https://gitlab.com/virtio-fs/virtiofsd/-/blob/main/src/fuse.rs?ref_type=heads#L1048
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InitOut {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub map_alignment: u16,
    pub flags2: u32,
    pub unused: [u32; 7],
}

/// We will use ByteValued to implement the encoding and decoding
/// of these structures in shared memory.
unsafe impl ByteValued for InHeader {}
unsafe impl ByteValued for OutHeader {}
unsafe impl ByteValued for InitIn {}
unsafe impl ByteValued for InitOut {}
