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

/// A helper function to desensitize secret string.
pub fn desensitize_secret(s: &str) -> String {
    // Always use 3 stars to mask the secret if length is low.
    //
    // # NOTE
    //
    // It's by design to use 10 instead of 6. Attackers could brute force the secrets
    // if the length is too short.
    if s.len() <= 10 {
        return "***".to_string();
    }

    // Keep the first & end three chars visible for easier debugging.
    format!("{}***{}", &s[..3], &s[s.len() - 3..])
}
