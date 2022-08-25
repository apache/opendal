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

//! Operations and help utils used by [`Accessor`][crate::Accessor].
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
pub use op_create::OpCreate;
mod op_delete;
pub use op_delete::OpDelete;
mod op_list;
pub use op_list::OpList;
mod op_presign;
pub use op_presign::OpPresign;
pub use op_presign::PresignedRequest;
mod op_read;
pub use op_read::OpRead;
mod op_stat;
pub use op_stat::OpStat;
mod op_write;
pub use op_write::OpWrite;
mod op_multipart;
pub use op_multipart::OpAbortMultipart;
pub use op_multipart::OpCompleteMultipart;
pub use op_multipart::OpCreateMultipart;
pub use op_multipart::OpWriteMultipart;
pub use op_multipart::Part;

mod bytes_range;
pub use bytes_range::BytesRange;
