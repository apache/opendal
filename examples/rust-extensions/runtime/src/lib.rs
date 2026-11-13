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

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};

use libloading::Library;
use opendal_core::raw::{Layer, Servicer};
use opendal_core::{OperationContext, OperatorRegistry};

pub use opendal_core as opendal;
pub use tokio;

pub type LayerFactory = fn() -> Arc<dyn Layer>;

/// Demo-only layer registry, independent of OpenDAL's operator registry.
#[derive(Default)]
pub struct LayerRegistry(Mutex<HashMap<String, LayerFactory>>);

impl LayerRegistry {
    pub fn get() -> &'static Self {
        static REGISTRY: LazyLock<LayerRegistry> = LazyLock::new(LayerRegistry::default);
        &REGISTRY
    }

    pub fn register(&self, name: &str, factory: LayerFactory) {
        self.0.lock().unwrap().insert(name.to_owned(), factory);
    }

    pub fn load(&self, name: &str) -> Option<DynamicLayer> {
        let factory = self.0.lock().unwrap().get(name).copied()?;
        Some(DynamicLayer(factory()))
    }
}

/// Adapts a native trait object to Operator::layer's generic argument.
#[derive(Debug)]
pub struct DynamicLayer(Arc<dyn Layer>);

impl Layer for DynamicLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        self.0.apply_service(inner)
    }

    fn apply_context(&self, service: Servicer, inner: OperationContext) -> OperationContext {
        self.0.apply_context(service, inner)
    }
}

type Register = unsafe extern "Rust" fn(&OperatorRegistry, &LayerRegistry);

/// Loads one trusted extension and retains its library for the process lifetime.
///
/// # Safety
/// The library must implement the entry point below and belong to the same build
/// as this runtime. This probe does not check binary compatibility. A matching
/// OpenDAL version alone is insufficient. Loading native code executes it.
pub unsafe fn load_extension(path: &Path) -> Result<(), libloading::Error> {
    let library = unsafe { Library::new(path)? };
    let register = unsafe { *library.get::<Register>(b"opendal_demo_register\0")? };
    // Factories, trait objects, futures, and buffers can outlive this function.
    // Retain the library before calling extension code, including if it panics.
    std::mem::forget(library);
    unsafe { register(OperatorRegistry::get(), LayerRegistry::get()) };
    Ok(())
}
