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

use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use libloading::{Library, Symbol};
use magnus::prelude::*;
use magnus::{Error, RString, Ruby, function, method};
use opendal_dynamic_extension_sdk::{
    ByteSlice, KeyValue, OutputBuffer, RuntimeApiV1, RuntimeProtocolInfoV1,
    STATUS_BUFFER_TOO_SMALL, STATUS_OK, ServiceRegistrationV1,
};

type GetRuntimeApiFn =
    unsafe extern "C" fn(u32, *mut RuntimeProtocolInfoV1, *mut *const RuntimeApiV1) -> i32;

struct NativeRuntime {
    _library: Library,
    api_address: usize,
    library_path: PathBuf,
    minimum_protocol: u32,
    protocol: u32,
}

impl NativeRuntime {
    fn api(&self) -> &RuntimeApiV1 {
        unsafe { &*(self.api_address as *const RuntimeApiV1) }
    }
}

static RUNTIME: OnceLock<NativeRuntime> = OnceLock::new();

fn runtime_error(ruby: &Ruby, message: impl AsRef<str>) -> Error {
    Error::new(ruby.exception_runtime_error(), message.as_ref().to_owned())
}

fn output(capacity: usize) -> (OutputBuffer, Vec<u8>) {
    let mut storage = vec![0; capacity];
    let buffer = OutputBuffer {
        data: storage.as_mut_ptr(),
        capacity: storage.len(),
        len: 0,
    };
    (buffer, storage)
}

fn output_message(buffer: &OutputBuffer, storage: &[u8]) -> String {
    String::from_utf8_lossy(&storage[..buffer.len.min(storage.len())]).into_owned()
}

fn byte_slice(value: &[u8]) -> ByteSlice {
    ByteSlice {
        data: value.as_ptr(),
        len: value.len(),
    }
}

fn require_runtime(ruby: &Ruby) -> Result<&'static NativeRuntime, Error> {
    RUNTIME
        .get()
        .ok_or_else(|| runtime_error(ruby, "OpenDAL runtime is not loaded"))
}

fn load_runtime(ruby: &Ruby, library_path: String, required_protocol: u32) -> Result<(), Error> {
    let library_path = Path::new(&library_path)
        .canonicalize()
        .map_err(|error| runtime_error(ruby, error.to_string()))?;
    if let Some(runtime) = RUNTIME.get() {
        if runtime.library_path != library_path {
            return Err(runtime_error(
                ruby,
                format!(
                    "runtime already loaded from {}",
                    runtime.library_path.display()
                ),
            ));
        }
        if required_protocol < runtime.minimum_protocol || required_protocol > runtime.protocol {
            return Err(runtime_error(ruby, "runtime protocol is incompatible"));
        }
        return Ok(());
    }

    let library = unsafe { Library::new(&library_path) }
        .map_err(|error| runtime_error(ruby, error.to_string()))?;
    let get_api: Symbol<'_, GetRuntimeApiFn> = unsafe {
        library
            .get(b"opendal_runtime_get_api_v1")
            .map_err(|error| runtime_error(ruby, error.to_string()))?
    };
    let mut protocol = RuntimeProtocolInfoV1 {
        struct_size: size_of::<RuntimeProtocolInfoV1>(),
        minimum_runtime_protocol: 0,
        runtime_protocol: 0,
    };
    let mut api = ptr::null();
    let status = unsafe { get_api(required_protocol, &mut protocol, &mut api) };
    if status != STATUS_OK || api.is_null() {
        return Err(runtime_error(
            ruby,
            format!(
                "runtime protocol negotiation failed: required={required_protocol}, supported={}..{}",
                protocol.minimum_runtime_protocol, protocol.runtime_protocol
            ),
        ));
    }

    RUNTIME
        .set(NativeRuntime {
            _library: library,
            api_address: api as usize,
            library_path,
            minimum_protocol: protocol.minimum_runtime_protocol,
            protocol: protocol.runtime_protocol,
        })
        .map_err(|_| runtime_error(ruby, "runtime was loaded concurrently"))
}

fn minimum_runtime_protocol(ruby: &Ruby) -> Result<u32, Error> {
    Ok(require_runtime(ruby)?.minimum_protocol)
}

fn runtime_protocol(ruby: &Ruby) -> Result<u32, Error> {
    Ok(require_runtime(ruby)?.protocol)
}

fn register_service(
    ruby: &Ruby,
    package_id: String,
    component_id: String,
    entry_symbol: String,
    library_path: String,
    required_protocol: u32,
) -> Result<(), Error> {
    let runtime = require_runtime(ruby)?;
    let package = byte_slice(package_id.as_bytes());
    let component = byte_slice(component_id.as_bytes());
    let entry = byte_slice(entry_symbol.as_bytes());
    let path = Path::new(&library_path)
        .canonicalize()
        .map_err(|error| runtime_error(ruby, error.to_string()))?;
    let path = path.to_string_lossy();
    let path_slice = byte_slice(path.as_bytes());
    let registration = ServiceRegistrationV1 {
        struct_size: size_of::<ServiceRegistrationV1>(),
        required_runtime_protocol: required_protocol,
        package_id: package,
        component_id: component,
        entry_symbol: entry,
        library_path: path_slice,
    };
    let (mut error, error_storage) = output(4096);
    let status = unsafe { (runtime.api().register_service)(&registration, &mut error) };
    if status != STATUS_OK {
        return Err(runtime_error(ruby, output_message(&error, &error_storage)));
    }
    Ok(())
}

#[magnus::wrap(class = "OpenDal::Operator", free_immediately)]
struct Operator {
    handle_address: AtomicUsize,
}

impl Drop for Operator {
    fn drop(&mut self) {
        self.close();
    }
}

impl Operator {
    fn close(&self) {
        let handle_address = self.handle_address.swap(0, Ordering::AcqRel);
        if handle_address == 0 {
            return;
        }
        if let Some(runtime) = RUNTIME.get() {
            unsafe {
                (runtime.api().operator_destroy)(handle_address as *mut c_void);
            }
        }
    }

    fn require_handle(&self, ruby: &Ruby) -> Result<*mut c_void, Error> {
        let handle_address = self.handle_address.load(Ordering::Acquire);
        if handle_address == 0 {
            return Err(runtime_error(ruby, "operator is closed"));
        }
        Ok(handle_address as *mut c_void)
    }

    fn new(
        ruby: &Ruby,
        scheme: String,
        options: Option<HashMap<String, String>>,
    ) -> Result<Self, Error> {
        let runtime = require_runtime(ruby)?;
        let scheme = scheme.trim().to_lowercase().replace('_', "-");
        let options = options.unwrap_or_default();
        let pairs: Vec<KeyValue> = options
            .iter()
            .map(|(key, value)| KeyValue {
                key: byte_slice(key.as_bytes()),
                value: byte_slice(value.as_bytes()),
            })
            .collect();
        let mut handle = ptr::null_mut();
        let (mut error, error_storage) = output(4096);
        let status = unsafe {
            (runtime.api().create_operator)(
                byte_slice(scheme.as_bytes()),
                pairs.as_ptr(),
                pairs.len(),
                &mut handle,
                &mut error,
            )
        };
        if status != STATUS_OK || handle.is_null() {
            return Err(runtime_error(ruby, output_message(&error, &error_storage)));
        }
        Ok(Self {
            handle_address: AtomicUsize::new(handle as usize),
        })
    }

    fn close_ruby(operator: &Self) {
        operator.close();
    }

    fn info_json(ruby: &Ruby, operator: &Self) -> Result<String, Error> {
        let runtime = require_runtime(ruby)?;
        let (mut result, result_storage) = output(4096);
        let (mut error, error_storage) = output(4096);
        let status = unsafe {
            (runtime.api().operator_info)(operator.require_handle(ruby)?, &mut result, &mut error)
        };
        if status != STATUS_OK {
            return Err(runtime_error(ruby, output_message(&error, &error_storage)));
        }
        Ok(output_message(&result, &result_storage))
    }

    fn write(ruby: &Ruby, operator: &Self, path: String, data: RString) -> Result<(), Error> {
        let runtime = require_runtime(ruby)?;
        let (mut error, error_storage) = output(4096);
        let data = data.to_bytes();
        let status = unsafe {
            (runtime.api().operator_write)(
                operator.require_handle(ruby)?,
                byte_slice(path.as_bytes()),
                byte_slice(&data),
                &mut error,
            )
        };
        if status != STATUS_OK {
            return Err(runtime_error(ruby, output_message(&error, &error_storage)));
        }
        Ok(())
    }

    fn read(ruby: &Ruby, operator: &Self, path: String) -> Result<bytes::Bytes, Error> {
        let runtime = require_runtime(ruby)?;
        let (mut result, mut result_storage) = output(4096);
        let (mut error, error_storage) = output(4096);
        let mut status = unsafe {
            (runtime.api().operator_read)(
                operator.require_handle(ruby)?,
                byte_slice(path.as_bytes()),
                &mut result,
                &mut error,
            )
        };
        if status == STATUS_BUFFER_TOO_SMALL {
            (result, result_storage) = output(result.len);
            status = unsafe {
                (runtime.api().operator_read)(
                    operator.require_handle(ruby)?,
                    byte_slice(path.as_bytes()),
                    &mut result,
                    &mut error,
                )
            };
        }
        if status != STATUS_OK {
            return Err(runtime_error(ruby, output_message(&error, &error_storage)));
        }
        result_storage.truncate(result.len);
        Ok(result_storage.into())
    }
}

#[magnus::init(name = "opendal_ruby_poc")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let opendal = ruby.define_module("OpenDal")?;
    let runtime = opendal.define_module("Runtime")?;
    runtime.define_singleton_method("load", function!(load_runtime, 2))?;
    runtime.define_singleton_method(
        "minimum_runtime_protocol",
        function!(minimum_runtime_protocol, 0),
    )?;
    runtime.define_singleton_method("runtime_protocol", function!(runtime_protocol, 0))?;
    runtime.define_singleton_method("register_service", function!(register_service, 5))?;

    let operator = opendal.define_class("Operator", ruby.class_object())?;
    operator.define_singleton_method("new", function!(Operator::new, 2))?;
    operator.define_method("close", method!(Operator::close_ruby, 0))?;
    operator.define_method("info_json", method!(Operator::info_json, 0))?;
    operator.define_method("write", method!(Operator::write, 2))?;
    operator.define_method("read", method!(Operator::read, 1))?;
    Ok(())
}
