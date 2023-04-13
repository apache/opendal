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

/// Macro used to generate operator upon construction and return a NULL [`opendal_operator_ptr`]
/// if failed. Examples can be seen in [`crate::opendal_operator_new`].
///
/// Arguments:
/// * cast_to: The builder type you want to cast to, e.g. [`opendal_builder_memory`]
/// * builder: The pointer to the builder
#[macro_export]
macro_rules! generate_operator {
    ($cast_to:ty, $builder:expr) => {{
        let builder = (*($builder as *mut $cast_to)).cast_to_builder();
        match od::Operator::new(builder) {
            Ok(b) => b.finish(),
            Err(_) => {
                return opendal_operator_ptr::null();
            }
        }
    }};
}
