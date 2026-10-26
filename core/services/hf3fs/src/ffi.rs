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

//! Minimal bindings to the public `hf3fs_usrbio.h` ABI.
//! https://github.com/deepseek-ai/3FS/blob/22fca04564c7cc230fd8b9523b8b92864e1dad47/src/lib/api/hf3fs_usrbio.h

use std::ffi::{CStr, c_char, c_int, c_void};
use std::path::Path;
use std::sync::Arc;

use libloading::Library;
use opendal_core::{Error, ErrorKind, Result};

#[repr(C)]
pub(crate) struct Iov {
    pub base: *mut u8,
    handle: *mut c_void,
    id: [c_char; 16],
    mount_point: [c_char; 256],
    size: usize,
    block_size: usize,
    numa: c_int,
}

#[repr(C)]
pub(crate) struct Ior {
    iov: Iov,
    handle: *mut c_void,
    mount_point: [c_char; 256],
    for_read: bool,
    io_depth: c_int,
    priority: c_int,
    timeout: c_int,
    flags: u64,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Cqe {
    pub index: i32,
    reserved: i32,
    pub result: i64,
    pub userdata: *const c_void,
}

pub(crate) struct Api {
    pub extract_mount_point: unsafe extern "C" fn(*mut c_char, c_int, *const c_char) -> c_int,
    pub iovcreate: unsafe extern "C" fn(*mut Iov, *const c_char, usize, usize, c_int) -> c_int,
    pub iovdestroy: unsafe extern "C" fn(*mut Iov),
    pub iorcreate4: unsafe extern "C" fn(
        *mut Ior,
        *const c_char,
        c_int,
        bool,
        c_int,
        c_int,
        c_int,
        u64,
    ) -> c_int,
    pub iordestroy: unsafe extern "C" fn(*mut Ior),
    pub reg_fd: unsafe extern "C" fn(c_int, u64) -> c_int,
    pub dereg_fd: unsafe extern "C" fn(c_int),
    pub prep_io: unsafe extern "C" fn(
        *const Ior,
        *const Iov,
        bool,
        *mut c_void,
        c_int,
        usize,
        u64,
        *const c_void,
    ) -> c_int,
    pub submit_ios: unsafe extern "C" fn(*const Ior) -> c_int,
    pub wait_for_ios:
        unsafe extern "C" fn(*const Ior, *mut Cqe, c_int, c_int, *const libc::timespec) -> c_int,
    _library: Library,
}

impl Api {
    pub fn load(path: &Path) -> Result<Arc<Self>> {
        // The configured library must implement the public 3FS C ABI. Keep it
        // loaded for every registered file, ring, and outstanding buffer.
        let load = || unsafe {
            let library = Library::new(path)?;
            Ok::<_, libloading::Error>(Self {
                extract_mount_point: *library.get(b"hf3fs_extract_mount_point\0")?,
                iovcreate: *library.get(b"hf3fs_iovcreate\0")?,
                iovdestroy: *library.get(b"hf3fs_iovdestroy\0")?,
                iorcreate4: *library.get(b"hf3fs_iorcreate4\0")?,
                iordestroy: *library.get(b"hf3fs_iordestroy\0")?,
                reg_fd: *library.get(b"hf3fs_reg_fd\0")?,
                dereg_fd: *library.get(b"hf3fs_dereg_fd\0")?,
                prep_io: *library.get(b"hf3fs_prep_io\0")?,
                submit_ios: *library.get(b"hf3fs_submit_ios\0")?,
                wait_for_ios: *library.get(b"hf3fs_wait_for_ios\0")?,
                _library: library,
            })
        };
        load().map(Arc::new).map_err(|e| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "failed to load 3FS USRBIO library",
            )
            .set_source(e)
        })
    }

    pub fn mount_point(&self, root: &CStr) -> Result<std::ffi::CString> {
        let mut mount = [0; 256];
        // Both strings are bounded and the root is NUL terminated.
        let n = unsafe { (self.extract_mount_point)(mount.as_mut_ptr(), 256, root.as_ptr()) };
        if n <= 0 || n > 256 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root is not on a supported 3FS mount",
            ));
        }
        // The API includes the terminating NUL in its return value.
        Ok(unsafe { CStr::from_ptr(mount.as_ptr()) }.to_owned())
    }
}
