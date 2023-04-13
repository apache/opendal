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

mod memory;

use ::opendal as od;

pub(crate) use memory::opendal_builder_memory;

/// The trait must be implemented for all builders in C binding.
///
/// IMPORTANT: Make sure that you consume and deallocate the &self in your implementation
/// of this trait. For example, in [`cast_to_builder`], using the following pattern is recommended:
/// ```ignore
/// fn cast_to_builder(&self) -> Self::CastTo {
///     // it will be dropped when the function ends
///     let boxed = unsafe { Box::from_raw(self as *const _ as *mut Self) };
///
///     // some other codes...
/// }
/// ```
/// This makes sure that the `boxed` is dropped when the function ends.
/// For more details, see [`crate::builders::memory::opendal_builder_memory`].
pub(crate) trait CastBuilder {
    /// The `CastTo` is the [`od::Builder`] type to be casted, e.g. [`od::services::Memory`]
    /// and [`od::services::Fs`]
    type CastTo: od::Builder;

    /// Deallocate the `&self` and returned the casted [`od::Builder`]
    fn cast_to_builder(&self) -> Self::CastTo;
}
