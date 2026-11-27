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

use reqsign_core::Context;

/// Create a reqsign [`Context`] for a signing service.
///
/// A [`reqsign_core::crypto::CryptoProvider`] installed with
/// [`reqsign_core::crypto::install_default`] before the operator is built is
/// used for every signature. Otherwise, with the `reqsign-rustcrypto` feature
/// enabled, the context is bound to the RustCrypto provider. Without either,
/// signing fails with [`reqsign_core::ErrorKind::ConfigInvalid`].
pub fn reqsign_context() -> Context {
    let ctx = Context::new();
    #[cfg(feature = "reqsign-rustcrypto")]
    let ctx = if reqsign_core::crypto::get_default().is_none() {
        ctx.with_crypto(reqsign_crypto_rustcrypto::RustCryptoProvider::new())
    } else {
        ctx
    };
    ctx
}

#[cfg(all(test, feature = "reqsign-rustcrypto"))]
mod tests {
    use super::*;

    #[test]
    fn binds_rustcrypto_without_a_process_wide_provider() {
        let ctx = reqsign_context();

        assert!(ctx.crypto_provider().is_ok());
    }
}
