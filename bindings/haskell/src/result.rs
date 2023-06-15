use std::{
    ffi::{c_char, CString},
    ptr,
};

#[repr(C)]
#[derive(Debug)]
pub struct FFIResult<T> {
    success: bool,
    data_ptr: *mut T,
    error_message: *mut c_char,
}

impl<T> FFIResult<T> {
    pub fn ok(data_ptr: *mut T) -> Self {
        FFIResult {
            success: true,
            data_ptr,
            error_message: ptr::null_mut(),
        }
    }

    pub fn err(error_message: &str) -> Self {
        let c_string = CString::new(error_message).unwrap();
        FFIResult {
            success: false,
            data_ptr: ptr::null_mut(),
            error_message: c_string.into_raw(),
        }
    }
}
