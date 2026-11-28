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

use std::sync::Arc;
use std::time::Duration;

use opendal_extension_demo_runtime::opendal::{OperatorRegistry, raw::Layer};
use opendal_extension_demo_runtime::{LayerRegistry, tokio};
use opendal_layer_timeout::TimeoutLayer;
use opendal_service_fs::Fs;

fn timeout_layer() -> Arc<dyn Layer> {
    Arc::new(
        TimeoutLayer::new()
            .with_timeout(Duration::from_secs(5))
            .with_io_timeout(Duration::from_millis(100)),
    )
}

#[unsafe(no_mangle)]
pub extern "Rust" fn opendal_demo_register(operators: &OperatorRegistry, layers: &LayerRegistry) {
    // These would differ if the extension had its own copies of the registries.
    assert!(std::ptr::eq(operators, OperatorRegistry::get()));
    assert!(std::ptr::eq(layers, LayerRegistry::get()));
    tokio::runtime::Handle::try_current().expect("host Tokio runtime must be visible");
    operators.register::<Fs>("demo-fs");
    layers.register("demo-timeout", timeout_layer);
}
