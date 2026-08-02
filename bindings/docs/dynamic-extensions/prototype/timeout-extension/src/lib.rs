// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

use opendal_core::Operator;
use opendal_layer_timeout::TimeoutLayer;

#[unsafe(no_mangle)]
/// Applies `TimeoutLayer` to an operator from the exact prototype build.
///
/// # Safety
///
/// `operator` must be a non-null pointer returned by the S3 extension from the
/// same compiler, target, profile, flags, source checkout, and lockfile.
pub unsafe extern "C" fn opendal_layer_timeout_bootstrap_v1(operator: *mut c_void) -> *mut c_void {
    if operator.is_null() {
        return std::ptr::null_mut();
    }

    catch_unwind(AssertUnwindSafe(|| {
        let operator = unsafe { Box::from_raw(operator.cast::<Operator>()) };
        Box::into_raw(Box::new(operator.layer(TimeoutLayer::default()))).cast::<c_void>()
    }))
    .unwrap_or_default()
}
