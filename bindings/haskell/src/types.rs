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

use std::ffi::CString;
use std::ffi::c_char;
use std::ptr;

use ::opendal as od;

#[repr(C)]
#[derive(Debug)]
pub struct ByteSlice {
    pub(crate) data: *mut c_char,
    pub(crate) len: usize,
}

impl ByteSlice {
    /// Transfer `vec` to the caller inside a `ByteSlice`.
    ///
    /// The vector is converted into a boxed slice first so the allocation
    /// length matches the reported `len` exactly; `free_byteslice` relies on
    /// this to rebuild the same `Box<[u8]>`.
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let boxed = vec.into_boxed_slice();
        let len = boxed.len();
        // Hand the whole allocation over: `into_raw` keeps the raw pointer's
        // provenance where `as_mut_ptr` + `mem::forget` would not.
        let data = Box::into_raw(boxed) as *mut u8 as *mut c_char;

        ByteSlice { data, len }
    }
}

/// Free a `ByteSlice` produced by `FFIResult<ByteSlice>`: first the payload
/// buffer, then the boxed `ByteSlice` itself.
///
/// # Safety
///
/// * `ptr` must be null or a `data_ptr` taken from an `FFIResult<ByteSlice>`
///   that has not been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_byteslice(ptr: *mut ByteSlice) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        let slice = Box::from_raw(ptr);
        if !slice.data.is_null() {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                slice.data as *mut u8,
                slice.len,
            )));
        }
    }
}

/// Free a `bool` produced by `FFIResult<bool>`.
///
/// # Safety
///
/// * `ptr` must be null or a `data_ptr` taken from an `FFIResult<bool>` that
///   has not been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_bool(ptr: *mut bool) {
    unsafe {
        if !ptr.is_null() {
            drop(Box::from_raw(ptr));
        }
    }
}

/// Free a nul-terminated string produced by this library: `error_message`
/// fields, `FFIResult<c_char>` payloads and `Metadata` string fields.
///
/// # Safety
///
/// * `ptr` must be null or a pointer returned by `into_raw_c_string` /
///   `CString::into_raw` in this crate that has not been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_string(ptr: *mut c_char) {
    unsafe {
        if !ptr.is_null() {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Free a `Metadata` produced by `FFIResult<Metadata>`: every string field,
/// then the boxed `Metadata` itself.
///
/// # Safety
///
/// * `ptr` must be null or a `data_ptr` taken from an `FFIResult<Metadata>`
///   that has not been freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_metadata(ptr: *mut Metadata) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        let metadata = Box::from_raw(ptr);
        free_string(metadata.cache_control);
        free_string(metadata.content_disposition);
        free_string(metadata.content_md5);
        free_string(metadata.content_type);
        free_string(metadata.etag);
        free_string(metadata.last_modified);
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum EntryMode {
    File,
    Dir,
    Unknown,
}

#[repr(C)]
#[derive(Debug)]
pub struct Metadata {
    pub(crate) mode: EntryMode,
    pub(crate) cache_control: *mut c_char,
    pub(crate) content_disposition: *mut c_char,
    pub(crate) content_length: u64,
    pub(crate) content_md5: *mut c_char,
    pub(crate) content_type: *mut c_char,
    pub(crate) etag: *mut c_char,
    pub(crate) last_modified: *mut c_char,
}

impl From<od::Metadata> for Metadata {
    fn from(val: od::Metadata) -> Self {
        let mode = match val.mode() {
            od::EntryMode::FILE => EntryMode::File,
            od::EntryMode::DIR => EntryMode::Dir,
            od::EntryMode::Unknown => EntryMode::Unknown,
        };

        Metadata {
            mode,
            cache_control: val
                .cache_control()
                .map_or(ptr::null_mut(), into_raw_c_string),
            content_disposition: val
                .content_disposition()
                .map_or(ptr::null_mut(), into_raw_c_string),
            content_length: val.content_length(),
            content_md5: val.content_md5().map_or(ptr::null_mut(), into_raw_c_string),
            content_type: val
                .content_type()
                .map_or(ptr::null_mut(), into_raw_c_string),
            etag: val.etag().map_or(ptr::null_mut(), into_raw_c_string),
            last_modified: val
                .last_modified()
                .map_or(ptr::null_mut(), |s| into_raw_c_string(&s.to_string())),
        }
    }
}

/// Convert `s` into a `CString` and leak it, transferring ownership to the
/// caller. The returned pointer must be released with `free_string`.
///
/// Interior NUL bytes are escaped as `\0` so this function never fails and
/// never aborts the process on adversarial input.
pub(crate) fn into_raw_c_string(s: &str) -> *mut c_char {
    let s = if s.contains('\0') {
        s.replace('\0', "\\0")
    } else {
        s.to_string()
    };
    match CString::new(s) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::ffi::c_uint;
    use std::mem::align_of;
    use std::mem::offset_of;
    use std::mem::size_of;

    const fn align_up(offset: usize, align: usize) -> usize {
        if align <= 1 {
            offset
        } else {
            offset.div_ceil(align) * align
        }
    }

    #[test]
    fn metadata_layout_matches_c_field_types() {
        assert_eq!(size_of::<EntryMode>(), size_of::<c_uint>());
        assert_eq!(align_of::<EntryMode>(), align_of::<c_uint>());

        let mode_offset = 0;
        let cache_control_offset =
            align_up(mode_offset + size_of::<c_uint>(), align_of::<*mut c_char>());
        let content_disposition_offset = align_up(
            cache_control_offset + size_of::<*mut c_char>(),
            align_of::<*mut c_char>(),
        );
        let content_length_offset = align_up(
            content_disposition_offset + size_of::<*mut c_char>(),
            align_of::<u64>(),
        );
        let content_md5_offset = align_up(
            content_length_offset + size_of::<u64>(),
            align_of::<*mut c_char>(),
        );
        let content_type_offset = align_up(
            content_md5_offset + size_of::<*mut c_char>(),
            align_of::<*mut c_char>(),
        );
        let etag_offset = align_up(
            content_type_offset + size_of::<*mut c_char>(),
            align_of::<*mut c_char>(),
        );
        let last_modified_offset = align_up(
            etag_offset + size_of::<*mut c_char>(),
            align_of::<*mut c_char>(),
        );
        let expected_align = align_of::<c_uint>()
            .max(align_of::<*mut c_char>())
            .max(align_of::<u64>());
        let expected_size = align_up(
            last_modified_offset + size_of::<*mut c_char>(),
            expected_align,
        );

        assert_eq!(offset_of!(Metadata, mode), mode_offset);
        assert_eq!(offset_of!(Metadata, cache_control), cache_control_offset);
        assert_eq!(
            offset_of!(Metadata, content_disposition),
            content_disposition_offset
        );
        assert_eq!(offset_of!(Metadata, content_length), content_length_offset);
        assert_eq!(offset_of!(Metadata, content_md5), content_md5_offset);
        assert_eq!(offset_of!(Metadata, content_type), content_type_offset);
        assert_eq!(offset_of!(Metadata, etag), etag_offset);
        assert_eq!(offset_of!(Metadata, last_modified), last_modified_offset);
        assert_eq!(align_of::<Metadata>(), expected_align);
        assert_eq!(size_of::<Metadata>(), expected_size);
    }

    #[test]
    fn byteslice_roundtrip_preserves_bytes() {
        let payload: Vec<u8> = (0u8..=255).collect();
        let slice = ByteSlice::from_vec(payload.clone());
        assert_eq!(slice.len, payload.len());
        unsafe {
            let view = std::slice::from_raw_parts(slice.data as *const u8, slice.len);
            assert_eq!(view, payload.as_slice());
            free_byteslice(Box::into_raw(Box::new(slice)));
        }
    }

    #[test]
    fn byteslice_empty_roundtrip() {
        let slice = ByteSlice::from_vec(Vec::new());
        assert_eq!(slice.len, 0);
        unsafe { free_byteslice(Box::into_raw(Box::new(slice))) };
    }

    #[test]
    fn byteslice_free_null_is_noop() {
        unsafe {
            free_byteslice(ptr::null_mut());
        }
    }

    #[test]
    fn bool_result_roundtrip() {
        let data_ptr = Box::into_raw(Box::new(true));
        unsafe {
            assert!(*data_ptr);
            free_bool(data_ptr);
            free_bool(ptr::null_mut());
        }
    }

    #[test]
    fn c_string_roundtrip_and_escaping() {
        unsafe {
            let ptr = into_raw_c_string("with space and 'quotes'");
            assert_eq!(
                CStr::from_ptr(ptr).to_str().unwrap(),
                "with space and 'quotes'"
            );
            free_string(ptr);

            let escaped = into_raw_c_string("a\0b");
            assert_eq!(CStr::from_ptr(escaped).to_bytes(), b"a\\0b");
            free_string(escaped);

            free_string(ptr::null_mut());
        }
    }

    #[test]
    fn metadata_free_releases_every_field() {
        let mut builder = od::MetadataBuilder::file(42);
        builder
            .cache_control("no-cache")
            .content_disposition("attachment")
            .content_md5("md5")
            .content_type("text/plain")
            .etag("etag");
        let ffi = Metadata::from(builder.build());
        let ptr = Box::into_raw(Box::new(ffi));
        unsafe {
            assert_eq!((*ptr).content_length, 42);
            free_metadata(ptr);
            free_metadata(ptr::null_mut());
        }
    }

    #[test]
    fn metadata_free_with_null_fields() {
        let ffi = Metadata::from(od::MetadataBuilder::unknown().build());
        let ptr = Box::into_raw(Box::new(ffi));
        unsafe { free_metadata(ptr) };
    }
}
