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

//! Operations and help utils used by [`Accessor`][crate::raw::Accessor].
//!
//! # Notes
//!
//! Users should not use struct or functions here except the following cases:
//!
//! - Implement a new service support.
//! - Implement a new Layer.

mod operation;
pub use operation::Operation;

mod op_create;
pub use op_create::RpCreate;
mod op_delete;
pub use op_delete::RpDelete;
mod op_list;
pub use op_list::RpList;
mod op_presign;
pub use op_presign::PresignedRequest;
pub use op_presign::RpPresign;
mod op_read;
pub use op_read::RpRead;
mod op_stat;
pub use op_stat::RpStat;
mod op_write;
pub use op_write::RpWrite;
mod op_multipart;
pub use op_multipart::RpAbortMultipart;
pub use op_multipart::RpCompleteMultipart;
pub use op_multipart::RpCreateMultipart;
pub use op_multipart::RpWriteMultipart;

mod bytes_range;
pub use bytes_range::BytesRange;

mod bytes_content_range;
pub use bytes_content_range::BytesContentRange;
