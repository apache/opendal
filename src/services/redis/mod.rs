// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const REDIS_CONTENT_KEY_PREFIX_V0: &str = "v0:c:";
const REDIS_META_KEY_PREFIX_V0: &str = "v0:m:";
const REDIS_CHILDREN_KEY_PREFIX_V0: &str = "v0:k:";

fn v0_content_prefix(abs_path: &str) -> String {
    format!("{}{}", REDIS_CONTENT_KEY_PREFIX_V0, abs_path)
}

fn v0_meta_prefix(abs_path: &str) -> String {
    format!("{}{}", REDIS_META_KEY_PREFIX_V0, abs_path)
}

fn v0_children_prefix(abs_path: &str) -> String {
    format!("{}{}", REDIS_CHILDREN_KEY_PREFIX_V0, abs_path)
}

mod backend;
mod dir_stream;
mod error;
