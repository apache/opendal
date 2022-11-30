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

mod layer;
pub use layer::CacheLayer;

mod accessor;
use accessor::CacheAccessor;

mod strategy;
use strategy::new_fixed_cache_reader;
use strategy::new_whole_cache_reader;
pub use strategy::CacheStrategy;

mod policy;
pub use policy::CacheFillMethod;
pub use policy::CachePolicy;
use policy::DefaultCachePolicy;
