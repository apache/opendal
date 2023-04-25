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
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::{JClass, JValue};
use jni::sys::jboolean;
use jni::sys::jlong;
use jni::{JNIEnv, JavaVM};
use tokio::runtime::{Builder, Runtime};

use opendal::BlockingOperator;
use opendal::Operator;
use opendal::Scheme;

thread_local! {
    static JAVA_VM: RefCell<Option<Arc<JavaVM>>> = RefCell::new(None);
    static JENV: RefCell<Option<*mut jni::sys::JNIEnv>> = RefCell::new(None);
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_getOperator(
    mut env: JNIEnv,
    _class: JClass,
    input: JString,
    params: JObject,
) -> jlong {
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string!")
        .into();

    let scheme = Scheme::from_str(&input).unwrap();

    let map = convert_map(&mut env, &params);
    if let Ok(operator) = build_operator(scheme, map) {
        let java_vm = Arc::new(env.get_java_vm().unwrap());
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(move || {
                JENV.with(|cell| {
                    let env = java_vm.attach_current_thread_as_daemon().unwrap();
                    *cell.borrow_mut() = Some(env.get_raw());
                });
                JAVA_VM.with(|cell| {
                    *cell.borrow_mut() = Some(java_vm.clone());
                });
            })
            .on_thread_stop(move || {
                JENV.with(|cell| {
                    *cell.borrow_mut() = None;
                });
                JAVA_VM.with(|cell| unsafe {
                    cell.borrow_mut().take().unwrap().detach_current_thread();
                });
            })
            .build()
            .unwrap();
        Box::into_raw(Box::new((operator, runtime))) as jlong
    } else {
        env.exception_clear().expect("Couldn't clear exception");
        env.throw_new(
            "java/lang/IllegalArgumentException",
            "Unsupported operator.",
        )
        .expect("Couldn't throw exception");
        0 as jlong
    }
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_asyncWrite(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut (Operator, Runtime),
    file: JString,
    content: JString,
    future: JObject,
) {
    let (op, runtime) = &mut *ptr;

    let file: String = env.get_string(&file).unwrap().into();
    let content: String = env.get_string(&content).unwrap().into();
    let future = env.new_global_ref(future).unwrap();

    let x = async move {
        op.write(&file, content).await.unwrap();
        JENV.with(|cell| {
            let env_ptr = cell.borrow().unwrap();
            let mut env = JNIEnv::from_raw(env_ptr).unwrap();

            let system = env.find_class("java/lang/System").unwrap();
            let out = env
                .get_static_field(system, "out", "Ljava/io/PrintStream;")
                .unwrap()
                .l()
                .unwrap();
            let message = env.new_string("rust: write to file").unwrap();
            env.call_method(
                out.as_ref(),
                "println",
                "(Ljava/lang/String;)V",
                &[JValue::Object(message.as_ref())],
            )
            .unwrap();

            let boolean_class = env.find_class("java/lang/Boolean").unwrap();
            let boolean = env
                .get_static_field(boolean_class, "TRUE", "Ljava/lang/Boolean;")
                .unwrap();

            let _ = env
                .call_method(
                    future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[boolean.borrow()],
                )
                .unwrap();

            let message = env.new_string("rust: complete java future").unwrap();
            env.call_method(
                out.as_ref(),
                "println",
                "(Ljava/lang/String;)V",
                &[JValue::Object(message.as_ref())],
            )
            .unwrap();
        });
    };
    runtime.spawn(x);
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_freeOperator(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = Box::from_raw(ptr);
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
    content: JString,
) {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let content: String = env
        .get_string(&content)
        .expect("Couldn't get java string!")
        .into();
    op.write(&file, content).unwrap();
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) -> JString<'local> {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let content = String::from_utf8(op.read(&file).unwrap()).expect("Couldn't convert to string");

    let output = env
        .new_string(content)
        .expect("Couldn't create java string!");
    output
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_stat(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
) -> jlong {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let metadata = op.stat(&file).unwrap();
    Box::into_raw(Box::new(metadata)) as jlong
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_isFile(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) -> jboolean {
    let metadata = &mut *ptr;
    metadata.is_file() as jboolean
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_getContentLength(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) -> jlong {
    let metadata = &mut *ptr;
    metadata.content_length() as jlong
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_freeStat(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = Box::from_raw(ptr);
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_delete<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    op.delete(&file).unwrap();
}

fn build_operator(
    scheme: opendal::Scheme,
    map: HashMap<String, String>,
) -> Result<opendal::Operator, opendal::Error> {
    use opendal::services::*;

    let op = match scheme {
        opendal::Scheme::Azblob => opendal::Operator::from_map::<Azblob>(map).unwrap().finish(),
        opendal::Scheme::Azdfs => opendal::Operator::from_map::<Azdfs>(map).unwrap().finish(),
        opendal::Scheme::Fs => opendal::Operator::from_map::<Fs>(map).unwrap().finish(),
        opendal::Scheme::Gcs => opendal::Operator::from_map::<Gcs>(map).unwrap().finish(),
        opendal::Scheme::Ghac => opendal::Operator::from_map::<Ghac>(map).unwrap().finish(),
        opendal::Scheme::Http => opendal::Operator::from_map::<Http>(map).unwrap().finish(),
        opendal::Scheme::Ipmfs => opendal::Operator::from_map::<Ipmfs>(map).unwrap().finish(),
        opendal::Scheme::Memory => opendal::Operator::from_map::<Memory>(map).unwrap().finish(),
        opendal::Scheme::Obs => opendal::Operator::from_map::<Obs>(map).unwrap().finish(),
        opendal::Scheme::Oss => opendal::Operator::from_map::<Oss>(map).unwrap().finish(),
        opendal::Scheme::S3 => opendal::Operator::from_map::<S3>(map).unwrap().finish(),
        opendal::Scheme::Webdav => opendal::Operator::from_map::<Webdav>(map).unwrap().finish(),
        opendal::Scheme::Webhdfs => opendal::Operator::from_map::<Webhdfs>(map)
            .unwrap()
            .finish(),

        _ => {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Scheme not supported",
            ));
        }
    };

    Ok(op)
}

fn convert_map(env: &mut JNIEnv, params: &JObject) -> HashMap<String, String> {
    let mut result: HashMap<String, String> = HashMap::new();
    let _ = JMap::from_env(env, params)
        .unwrap()
        .iter(env)
        .and_then(|mut iter| {
            while let Some(e) = iter.next(env)? {
                let key = JString::from(e.0);
                let value = JString::from(e.1);
                let key: String = env
                    .get_string(&key)
                    .expect("Couldn't get java string!")
                    .into();
                let value: String = env
                    .get_string(&value)
                    .expect("Couldn't get java string!")
                    .into();
                result.insert(key, value);
            }
            Ok(())
        });
    result
}
