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

#![cfg(feature = "reqsign-rustcrypto")]

use std::sync::Arc;

use opendal_core::raw::reqsign_context;
use opendal_core::reqsign_core::crypto::{self, CryptoProvider};
use reqsign_crypto_rustcrypto::RustCryptoProvider;

#[test]
fn process_wide_provider_takes_precedence() {
    crypto::install_default(RustCryptoProvider::new()).unwrap();
    let installed: &Arc<dyn CryptoProvider> = crypto::get_default().unwrap();

    let ctx = reqsign_context();
    let used = ctx.crypto_provider().unwrap();

    assert!(std::ptr::addr_eq(
        used as *const dyn CryptoProvider,
        Arc::as_ptr(installed)
    ));
}
