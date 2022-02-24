// Copyright 2021 Datafuse Labs.
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
mod accessor;
pub use accessor::Accessor;

mod io;
pub use io::BoxedAsyncRead;
pub use io::Reader;
pub use io::Writer;

mod layer;
pub use layer::Layer;

mod operator;
pub use operator::Operator;

mod object;
pub use object::Metadata;
pub use object::Object;

mod scheme;
pub use scheme::Scheme;

pub mod credential;
pub mod error;
pub mod readers;

pub mod ops;
pub mod services;

#[cfg(test)]
mod tests;
