// Copyright 2022 Datafuse Labs
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

/// BlockingReader is a boxed dyn of [`BlockingRead`];
///
/// We use [`BlockingReader`] to accept users input in `Accessor` trait.
pub type BlockingReader = Box<dyn BlockingRead>;

/// BlockingRead is a trait alias of [`std::io::Read`] to avoid repeating
/// `std::io::Read + Send` across the codebase.
///
/// We use [`BlockingRead`] to accept users input.
pub trait BlockingRead: std::io::Read + Send {}
impl<T> BlockingRead for T where T: std::io::Read + Send {}
