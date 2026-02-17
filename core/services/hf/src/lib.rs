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

/// Primary scheme for the Hugging Face service.
pub const HF_SCHEME: &str = "hf";

/// Alias scheme for the Hugging Face service.
pub const HUGGINGFACE_SCHEME: &str = "huggingface";

/// Register this service into the given registry under both `hf` and `huggingface` schemes.
pub fn register_hf_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<Hf>(HF_SCHEME);
    registry.register::<Hf>(HUGGINGFACE_SCHEME);
}

mod backend;
mod config;
mod core;
mod error;
mod lister;

pub use backend::HfBuilder as Hf;
pub use config::HfConfig;

// Backward-compatible aliases.
#[doc(hidden)]
pub type Huggingface = Hf;
#[doc(hidden)]
pub type HuggingfaceConfig = HfConfig;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_aliases_are_interchangeable() {
        let _: Huggingface = Hf::default().repo_id("org/repo");
        let _: HuggingfaceConfig = HfConfig::default();
    }

    #[test]
    fn scheme_constants() {
        assert_eq!(HF_SCHEME, "hf");
        assert_eq!(HUGGINGFACE_SCHEME, "huggingface");
    }
}
