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

/// Macro used to generate operator upon construction and return C-compatible
/// error if failed
#[macro_export]
macro_rules! generate_operator {
    ($type:ty, $map:expr) => {{
        let b = od::Operator::from_map::<$type>($map);
        match b {
            Ok(b) => b.finish(),
            Err(_) => {
                return opendal_operator_ptr::null();
            }
        }
    }};
}
