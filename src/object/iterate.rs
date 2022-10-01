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

use std::io::Result;

use crate::ObjectEntry;

/// ObjectIterate represents an iterator of Dir.
pub trait ObjectIterate: Iterator<Item = Result<ObjectEntry>> {}
impl<T> ObjectIterate for T where T: Iterator<Item = Result<ObjectEntry>> {}

/// ObjectIterator is a boxed dyn [`ObjectIterate`]
pub type ObjectIterator = Box<dyn ObjectIterate>;

/// EmptyObjectIterator that always return None.
pub(crate) struct EmptyObjectIterator;

impl Iterator for EmptyObjectIterator {
    type Item = Result<ObjectEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
