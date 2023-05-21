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
use std::ffi::c_void;
use std::str::FromStr;

use jni::objects::JClass;
use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JThrowable;
use jni::objects::JValue;
use jni::sys::{jboolean, jobject, JNI_VERSION_1_8};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use once_cell::sync::OnceCell;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use opendal::Operator;
use opendal::Scheme;
use opendal::{BlockingOperator, ErrorKind};

static mut RUNTIME: OnceCell<Runtime> = OnceCell::new();
thread_local! {
    static ENV: RefCell<Option<*mut jni::sys::JNIEnv>> = RefCell::new(None);
}

/// # Safety
///
/// This function could be only called by java vm when load this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnLoad(vm: JavaVM, _: *mut c_void) -> jint {
    RUNTIME
        .set(
            Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .on_thread_start(move || {
                    ENV.with(|cell| {
                        let env = vm.attach_current_thread_as_daemon().unwrap();
                        *cell.borrow_mut() = Some(env.get_raw());
                    })
                })
                .build()
                .unwrap(),
        )
        .unwrap();

    JNI_VERSION_1_8
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnUnload(_: JavaVM, _: *mut c_void) {
    if let Some(r) = RUNTIME.take() {
        r.shutdown_background()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_newOperator(
    mut env: JNIEnv,
    _class: JClass,
    input: JString,
    params: JObject,
) -> jlong {
    let input: String = env
        .get_string(&input)
        .expect("cannot get java string")
        .into();

    let scheme = Scheme::from_str(&input).unwrap();

    let map = convert_jmap_to_hashmap(&mut env, &params);
    if let Ok(operator) = new_operator(scheme, map) {
        Box::into_raw(Box::new(operator)) as jlong
    } else {
        env.exception_clear().expect("cannot clear exception");
        env.throw_new(
            "java/lang/IllegalArgumentException",
            "Unsupported operator.",
        )
        .expect("cannot throw exception");
        0 as jlong
    }
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_writeAsync(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
    file: JString,
    content: JString,
) -> jobject {
    let op = &mut *ptr;

    let file: String = env.get_string(&file).unwrap().into();
    let content: String = env.get_string(&content).unwrap().into();

    let class = "java/util/concurrent/CompletableFuture";
    let f = env.new_object(class, "()V", &[]).unwrap();

    // keep the future alive, so that we can complete it later
    // but this approach will be limited by global ref table size (65535)
    let future = env.new_global_ref(&f).unwrap();

    RUNTIME.get_unchecked().spawn(async move {
        let result = op.write(&file, content).await;

        let env = ENV.with(|cell| *cell.borrow_mut()).unwrap();
        let mut env = JNIEnv::from_raw(env).unwrap();

        match result {
            Ok(()) => env
                .call_method(
                    future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&JObject::null())],
                )
                .unwrap(),
            Err(err) => {
                let exception = convert_error_to_exception(&mut env, err).unwrap();
                env.call_method(
                    future,
                    "completeExceptionally",
                    "(Ljava/lang/Throwable;)Z",
                    &[JValue::Object(&exception)],
                )
                .unwrap()
            }
        }
    });

    f.as_raw()
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_disposeInternal(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
) {
    drop(Box::from_raw(ptr));
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
        .expect("cannot get java string!")
        .into();
    let content: String = env
        .get_string(&content)
        .expect("cannot get java string!")
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
        .expect("cannot get java string!")
        .into();
    let content = String::from_utf8(op.read(&file).unwrap()).expect("cannot convert to string");
    env.new_string(content).expect("cannot create java string")
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
        .expect("cannot get java string!")
        .into();
    let result = op.stat(&file);
    if let Err(error) = result {
        let exception = convert_error_to_exception(&mut env, error).unwrap();
        env.throw(exception).unwrap();
        return 0 as jlong;
    }
    Box::into_raw(Box::new(result.unwrap())) as jlong
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
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_disposeInternal(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) {
    drop(Box::from_raw(ptr));
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
        .expect("cannot get java string!")
        .into();
    op.delete(&file).unwrap();
}

/// # Safety
///
/// This function should be called by JNI only.
#[cfg(feature = "tests")]
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_ExceptionTest_getErrors(
    env: JNIEnv,
    _: JClass,
) -> jni::sys::jbyteArray {
    use enum_iterator::all;

    let errors = all::<ErrorKind>()
        .map(|k| k.into_ordinal())
        .collect::<Vec<_>>();

    env.byte_array_from_slice(&errors).unwrap().into_raw()
}

/// # Safety
///
/// This function should be called by JNI only.
#[cfg(feature = "tests")]
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_ExceptionTest_getErrorName(
    env: JNIEnv,
    _: JClass,
    ordinal: jni::sys::jbyte,
) -> jni::sys::jstring {
    use enum_iterator::all;

    let name = all::<ErrorKind>()
        .find(|k| (k.into_ordinal() as i8) == ordinal)
        .unwrap();
    env.new_string(name.to_string()).unwrap().into_raw()
}

fn new_operator(scheme: Scheme, map: HashMap<String, String>) -> Result<Operator, opendal::Error> {
    use opendal::services::*;

    let op = match scheme {
        Scheme::Azblob => Operator::from_map::<Azblob>(map).unwrap().finish(),
        Scheme::Azdfs => Operator::from_map::<Azdfs>(map).unwrap().finish(),
        Scheme::Fs => Operator::from_map::<Fs>(map).unwrap().finish(),
        Scheme::Gcs => Operator::from_map::<Gcs>(map).unwrap().finish(),
        Scheme::Ghac => Operator::from_map::<Ghac>(map).unwrap().finish(),
        Scheme::Http => Operator::from_map::<Http>(map).unwrap().finish(),
        Scheme::Ipmfs => Operator::from_map::<Ipmfs>(map).unwrap().finish(),
        Scheme::Memory => Operator::from_map::<Memory>(map).unwrap().finish(),
        Scheme::Obs => Operator::from_map::<Obs>(map).unwrap().finish(),
        Scheme::Oss => Operator::from_map::<Oss>(map).unwrap().finish(),
        Scheme::S3 => Operator::from_map::<S3>(map).unwrap().finish(),
        Scheme::Webdav => Operator::from_map::<Webdav>(map).unwrap().finish(),
        Scheme::Webhdfs => Operator::from_map::<Webhdfs>(map).unwrap().finish(),

        _ => {
            return Err(opendal::Error::new(
                ErrorKind::Unexpected,
                format!("scheme {scheme:?} not supported"),
            ));
        }
    };

    Ok(op)
}

fn convert_error_to_exception<'local>(
    env: &mut JNIEnv<'local>,
    error: opendal::Error,
) -> Result<JThrowable<'local>, jni::errors::Error> {
    let class = env.find_class("org/apache/opendal/exception/ODException")?;

    let code = error.kind().into_ordinal() as i8;
    let message = env.new_string(error.to_string())?;

    let sig = "(BLjava/lang/String;)V";
    let params = &[JValue::Byte(code), JValue::Object(&message)];
    env.new_object(class, sig, params).map(JThrowable::from)
}

fn convert_jmap_to_hashmap(env: &mut JNIEnv, params: &JObject) -> HashMap<String, String> {
    let map = JMap::from_env(env, params).unwrap();
    let mut iter = map.iter(env).unwrap();

    let mut result: HashMap<String, String> = HashMap::new();
    while let Some(e) = iter.next(env).unwrap() {
        let k = JString::from(e.0);
        let v = JString::from(e.1);
        result.insert(
            env.get_string(&k).unwrap().into(),
            env.get_string(&v).unwrap().into(),
        );
    }
    result
}
