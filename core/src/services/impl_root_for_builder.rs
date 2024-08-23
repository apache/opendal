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

/// A helper macro to impl root methods for all service builders.
#[macro_export]
macro_rules! impl_root_for_builder {
    ($(#[$attr:meta] $(#[doc = $doc:expr])* $struct_name:path),*$(,)?) => {
      $(
        #[$attr]
        impl $struct_name {
            /// Set root of this backend.
            ///
            /// All operations will happen under this root.
            $(#[doc = $doc])*
            pub fn root(mut self, root: &str) -> Self {
                self.config.root = if root.is_empty() {
                    None
                } else {
                    Some(root.to_string())
                };

                self
            }
        }
    )*
    };

    ($($(#[doc = $doc:expr])* $struct_name:path),* $(,)?) => {
        $(
            impl $struct_name {
                /// Set root of this backend.
                ///
                /// All operations will happen under this root.
                $(#[doc = $doc])*
                pub fn root(mut self, root: &str) -> Self {
                    self.config.root = if root.is_empty() {
                        None
                    } else {
                        Some(root.to_string())
                    };

                    self
                }
            }
    )*
    };
}
