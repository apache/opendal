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

//! `output` provides traits and types that opendal returns as output.
//!
//! Unlike traits provided by `input`, we could add more features. For
//! example, we only requires `Send` for `input::Reader` but we provide
//! `Send + Sync` for `output::Reader` which makes it easier for user to use.
//!
//! Those types should only be used internally and we don't want users to
//! depend on them. So we should also implement trait like `AsyncRead` for
//! our `output` traits.

mod read;
pub use read::Read;
pub use read::ReadExt;
pub use read::Reader;

pub mod into_reader;

mod blocking_read;
pub use blocking_read::BlockingRead;
pub use blocking_read::BlockingReader;

pub mod into_blocking_reader;

mod cursor;
pub use cursor::Cursor;

mod into_streamable;
pub use into_streamable::into_streamable_reader;
pub use into_streamable::IntoStreamableReader;

mod entry;
pub use entry::Entry;

mod page;
pub use page::BlockingPage;
pub use page::BlockingPager;
pub use page::Page;
pub use page::Pager;

mod to_flat_pager;
pub use to_flat_pager::to_flat_pager;
pub use to_flat_pager::ToFlatPager;

mod to_hierarchy_pager;
pub use to_hierarchy_pager::to_hierarchy_pager;
pub use to_hierarchy_pager::ToHierarchyPager;
