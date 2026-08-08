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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock, Mutex};

use libloading::{Library, Symbol};
use opendal_dynamic_extension_sdk::{
    ByteSlice, CreateOperatorFn, DestroyOperatorFn, ExtensionApiV1, ExtensionBootstrapV1, KeyValue,
    OperatorInfoFn, OperatorReadFn, OperatorWriteFn, OutputBuffer, RUNTIME_PROTOCOL, RuntimeApiV1,
    RuntimeProtocolInfoV1, STATUS_CONFLICT, STATUS_INCOMPATIBLE, STATUS_INVALID_ARGUMENT,
    STATUS_LOAD_FAILED, STATUS_OK, STATUS_OPERATION_FAILED, ServiceRegistrationV1, write_output,
};

struct LoadedService {
    _library: Library,
    create_operator: CreateOperatorFn,
    destroy_operator: DestroyOperatorFn,
    operator_info: OperatorInfoFn,
    operator_write: OperatorWriteFn,
    operator_read: OperatorReadFn,
}

struct RegisteredService {
    package_id: String,
    component_id: String,
    entry_symbol: String,
    library_path: String,
    loaded: Mutex<Option<Arc<LoadedService>>>,
}

struct RuntimeOperator {
    operator: *mut c_void,
    service: Arc<LoadedService>,
}

impl Drop for RuntimeOperator {
    fn drop(&mut self) {
        unsafe { (self.service.destroy_operator)(self.operator) };
    }
}

static SERVICES: LazyLock<Mutex<HashMap<String, Arc<RegisteredService>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static RUNTIME_API: RuntimeApiV1 = RuntimeApiV1 {
    struct_size: size_of::<RuntimeApiV1>(),
    register_service,
    create_operator,
    operator_info,
    operator_write,
    operator_read,
    operator_destroy,
};

fn fail(error: *mut OutputBuffer, status: i32, message: impl AsRef<str>) -> i32 {
    let _ = unsafe { write_output(error, message.as_ref().as_bytes()) };
    status
}

fn required_struct<T>(actual: usize, name: &str) -> Result<(), String> {
    if actual < size_of::<T>() {
        return Err(format!("{name} is smaller than the version 1 layout"));
    }
    Ok(())
}

unsafe fn load_service(
    registration: &RegisteredService,
) -> Result<Arc<LoadedService>, (i32, String)> {
    let mut loaded = registration.loaded.lock().map_err(|_| {
        (
            STATUS_OPERATION_FAILED,
            "service load mutex poisoned".to_string(),
        )
    })?;
    if let Some(service) = loaded.as_ref() {
        return Ok(Arc::clone(service));
    }

    let library = unsafe { Library::new(&registration.library_path) }
        .map_err(|err| (STATUS_LOAD_FAILED, err.to_string()))?;
    let bootstrap: Symbol<'_, ExtensionBootstrapV1> =
        unsafe { library.get(registration.entry_symbol.as_bytes()) }
            .map_err(|err| (STATUS_LOAD_FAILED, err.to_string()))?;
    let mut extension = std::ptr::null();
    let status = unsafe { bootstrap(&mut extension) };
    if status != STATUS_OK || extension.is_null() {
        return Err((STATUS_LOAD_FAILED, "extension bootstrap failed".to_string()));
    }
    let extension: &ExtensionApiV1 = unsafe { &*extension };
    required_struct::<ExtensionApiV1>(extension.struct_size, "extension API")
        .map_err(|message| (STATUS_INCOMPATIBLE, message))?;
    if extension.required_runtime_protocol > RUNTIME_PROTOCOL {
        return Err((
            STATUS_INCOMPATIBLE,
            "extension requires a newer runtime protocol".to_string(),
        ));
    }

    let extension_version = unsafe { extension.opendal_version.as_str() }
        .map_err(|message| (STATUS_INCOMPATIBLE, message.to_string()))?;
    if extension_version != opendal_core::raw::VERSION {
        return Err((
            STATUS_INCOMPATIBLE,
            format!(
                "extension OpenDAL version {extension_version} does not match runtime {}",
                opendal_core::raw::VERSION
            ),
        ));
    }
    let extension_package = unsafe { extension.package_id.as_str() }
        .map_err(|message| (STATUS_INCOMPATIBLE, message.to_string()))?;
    if extension_package != registration.package_id {
        return Err((
            STATUS_INCOMPATIBLE,
            "extension package does not match registration".to_string(),
        ));
    }
    let extension_component = unsafe { extension.component_id.as_str() }
        .map_err(|message| (STATUS_INCOMPATIBLE, message.to_string()))?;
    if extension_component != registration.component_id {
        return Err((
            STATUS_INCOMPATIBLE,
            "extension component does not match registration".to_string(),
        ));
    }
    let extension_entry = unsafe { extension.entry_symbol.as_str() }
        .map_err(|message| (STATUS_INCOMPATIBLE, message.to_string()))?;
    if extension_entry != registration.entry_symbol {
        return Err((
            STATUS_INCOMPATIBLE,
            "extension entry symbol does not match registration".to_string(),
        ));
    }

    let service = Arc::new(LoadedService {
        _library: library,
        create_operator: extension.create_operator,
        destroy_operator: extension.destroy_operator,
        operator_info: extension.operator_info,
        operator_write: extension.operator_write,
        operator_read: extension.operator_read,
    });
    *loaded = Some(Arc::clone(&service));
    Ok(service)
}

unsafe extern "C" fn register_service(
    registration: *const ServiceRegistrationV1,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if registration.is_null() {
            return fail(error, STATUS_INVALID_ARGUMENT, "registration is null");
        }
        let registration = &*registration;
        if let Err(message) = required_struct::<ServiceRegistrationV1>(
            registration.struct_size,
            "service registration",
        ) {
            return fail(error, STATUS_INVALID_ARGUMENT, message);
        }
        if registration.required_runtime_protocol > RUNTIME_PROTOCOL {
            return fail(
                error,
                STATUS_INCOMPATIBLE,
                "service requires a newer runtime protocol",
            );
        }

        let package_id = match registration.package_id.as_str() {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => return fail(error, STATUS_INVALID_ARGUMENT, "package ID is empty"),
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        let component_id = match registration.component_id.as_str() {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => return fail(error, STATUS_INVALID_ARGUMENT, "component ID is empty"),
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        let entry_symbol = match registration.entry_symbol.as_str() {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => return fail(error, STATUS_INVALID_ARGUMENT, "entry symbol is empty"),
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        let library_path = match registration.library_path.as_str() {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => return fail(error, STATUS_INVALID_ARGUMENT, "library path is empty"),
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };

        let mut services = SERVICES.lock().expect("service registry mutex poisoned");
        if let Some(existing) = services.get(component_id) {
            if existing.package_id == package_id {
                return STATUS_OK;
            }
            return fail(
                error,
                STATUS_CONFLICT,
                format!(
                    "service {component_id} is already owned by {}",
                    existing.package_id
                ),
            );
        }
        services.insert(
            component_id.to_string(),
            Arc::new(RegisteredService {
                package_id: package_id.to_string(),
                component_id: component_id.to_string(),
                entry_symbol: entry_symbol.to_string(),
                library_path: library_path.to_string(),
                loaded: Mutex::new(None),
            }),
        );
        STATUS_OK
    }))
    .unwrap_or_else(|_| {
        fail(
            error,
            STATUS_OPERATION_FAILED,
            "service registration panicked",
        )
    })
}

unsafe extern "C" fn create_operator(
    component_id: ByteSlice,
    options: *const KeyValue,
    options_len: usize,
    operator: *mut *mut c_void,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if operator.is_null() {
            return fail(error, STATUS_INVALID_ARGUMENT, "operator output is null");
        }
        *operator = std::ptr::null_mut();
        if options_len > 0 && options.is_null() {
            return fail(error, STATUS_INVALID_ARGUMENT, "options pointer is null");
        }
        let component_id = match component_id.as_str() {
            Ok(value) => value,
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        let registration = {
            let services = SERVICES.lock().expect("service registry mutex poisoned");
            match services.get(component_id) {
                Some(registration) => Arc::clone(registration),
                None => {
                    return fail(
                        error,
                        STATUS_INVALID_ARGUMENT,
                        format!(
                            "service {component_id} is not registered; import its package first"
                        ),
                    );
                }
            }
        };
        let service = match load_service(&registration) {
            Ok(service) => service,
            Err((status, message)) => return fail(error, status, message),
        };

        let mut inner = std::ptr::null_mut();
        let status = (service.create_operator)(options, options_len, &mut inner, error);
        if status != STATUS_OK {
            return status;
        }
        if inner.is_null() {
            return fail(
                error,
                STATUS_OPERATION_FAILED,
                "service returned a null operator",
            );
        }
        *operator = Box::into_raw(Box::new(RuntimeOperator {
            operator: inner,
            service,
        }))
        .cast::<c_void>();
        STATUS_OK
    }))
    .unwrap_or_else(|_| fail(error, STATUS_OPERATION_FAILED, "operator creation panicked"))
}

unsafe fn runtime_operator<'a>(operator: *mut c_void) -> Result<&'a RuntimeOperator, &'static str> {
    if operator.is_null() {
        return Err("operator handle is null");
    }
    Ok(unsafe { &*operator.cast::<RuntimeOperator>() })
}

unsafe extern "C" fn operator_info(
    operator: *mut c_void,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match runtime_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        (operator.service.operator_info)(operator.operator, output, error)
    }))
    .unwrap_or_else(|_| fail(error, STATUS_OPERATION_FAILED, "operator info panicked"))
}

unsafe extern "C" fn operator_write(
    operator: *mut c_void,
    path: ByteSlice,
    data: ByteSlice,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match runtime_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        (operator.service.operator_write)(operator.operator, path, data, error)
    }))
    .unwrap_or_else(|_| fail(error, STATUS_OPERATION_FAILED, "operator write panicked"))
}

unsafe extern "C" fn operator_read(
    operator: *mut c_void,
    path: ByteSlice,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match runtime_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, STATUS_INVALID_ARGUMENT, message),
        };
        (operator.service.operator_read)(operator.operator, path, output, error)
    }))
    .unwrap_or_else(|_| fail(error, STATUS_OPERATION_FAILED, "operator read panicked"))
}

unsafe extern "C" fn operator_destroy(operator: *mut c_void) {
    if operator.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(operator.cast::<RuntimeOperator>()));
    }));
}

#[unsafe(no_mangle)]
/// Negotiates the runtime protocol and returns the runtime function table.
///
/// # Safety
///
/// `protocol_info` and `api` must point to writable storage. The caller must
/// initialize `protocol_info.struct_size` before calling this function.
pub unsafe extern "C" fn opendal_runtime_get_api_v1(
    required_runtime_protocol: u32,
    protocol_info: *mut RuntimeProtocolInfoV1,
    api: *mut *const RuntimeApiV1,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if protocol_info.is_null() || api.is_null() {
            return STATUS_INVALID_ARGUMENT;
        }
        let protocol_info = &mut *protocol_info;
        if protocol_info.struct_size < size_of::<RuntimeProtocolInfoV1>() {
            return STATUS_INVALID_ARGUMENT;
        }
        protocol_info.minimum_runtime_protocol = RUNTIME_PROTOCOL;
        protocol_info.runtime_protocol = RUNTIME_PROTOCOL;
        *api = std::ptr::null();
        if required_runtime_protocol != RUNTIME_PROTOCOL {
            return STATUS_INCOMPATIBLE;
        }
        *api = &RUNTIME_API;
        STATUS_OK
    }))
    .unwrap_or(STATUS_OPERATION_FAILED)
}
