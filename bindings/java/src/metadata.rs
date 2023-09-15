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

use jni::objects::JClass;
use jni::objects::JObject;
use jni::sys::jboolean;
use jni::sys::jlong;
use jni::JNIEnv;
use opendal::Metadata;

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_isFile(
    _: JNIEnv,
    _: JClass,
    ptr: *mut Metadata,
) -> jboolean {
    let metadata = &mut *ptr;
    metadata.is_file() as jboolean
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_getContentLength(
    _: JNIEnv,
    _: JClass,
    ptr: *mut Metadata,
) -> jlong {
    let metadata = &mut *ptr;
    metadata.content_length() as jlong
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_disposeInternal(
    _: JNIEnv,
    _: JObject,
    ptr: *mut Metadata,
) {
    drop(Box::from_raw(ptr));
}
