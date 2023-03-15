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

use core::slice;

use std::ffi::CStr;

use opendal as od;
use opendal::services::Memory;

pub struct BlockingOperator {
    op: od::BlockingOperator,
}

#[repr(C)]
pub struct DynArray {
    array: *mut u8,
    length: libc::size_t,
}

/// Hello, OpenDAL!
#[no_mangle]
pub extern "C" fn hello_opendal() {
    let op = od::Operator::new(Memory::default()).unwrap().finish();
    println!("{op:?}")
}

#[no_mangle]
pub extern "C" fn blocking_op_create() -> *mut BlockingOperator {
    let op = od::Operator::new(od::services::Memory::default())
        .unwrap()
        .finish()
        .blocking();

    Box::into_raw(Box::new(BlockingOperator { op }))
}

#[no_mangle]
pub extern "C" fn blocking_read(op: &BlockingOperator, path: &libc::c_char) -> DynArray {
    let path = unsafe { CStr::from_ptr(path) };
    let path = path.to_str().unwrap();
    let mut read_result = op.op.read(path).unwrap();
    let result = DynArray {
        array: read_result.as_mut_ptr(),
        length: read_result.len() as _,
    };

    std::mem::forget(read_result);

    result
}

#[no_mangle]
pub extern "C" fn blocking_write(op: &BlockingOperator, path: &libc::c_char, bs: &DynArray) {
    let path = unsafe { CStr::from_ptr(path) };
    let path = path.to_str().unwrap();
    let bs = unsafe { slice::from_raw_parts(bs.array, bs.length) };
    let bs: Vec<u8> = Vec::from(bs);
    op.op.write(path, bs).unwrap();
}
