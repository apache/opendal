// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

use opendal_core::Operator;
use opendal_service_s3::S3;

#[unsafe(no_mangle)]
pub extern "C" fn opendal_service_s3_bootstrap_v1() -> *mut c_void {
    catch_unwind(AssertUnwindSafe(|| {
        match Operator::new(S3::default().bucket("prototype-bucket").region("us-east-1")) {
            Ok(operator) => Box::into_raw(Box::new(operator)).cast::<c_void>(),
            Err(err) => {
                eprintln!("failed to construct prototype S3 operator: {err}");
                std::ptr::null_mut()
            }
        }
    }))
    .unwrap_or_default()
}
