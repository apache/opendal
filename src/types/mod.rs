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

mod mode;
pub use mode::EntryMode;

mod metadata;
pub use metadata::Metadata;
pub use metadata::Metakey;

#[allow(clippy::module_inception)]
mod object;
pub use object::Object;

mod reader;
pub use reader::BlockingReader;
pub use reader::Reader;

mod writer;
pub use writer::BlockingWriter;
pub use writer::Writer;

mod list;
pub use list::BlockingLister;
pub use list::Lister;

mod operator;
pub use operator::BatchOperator;
pub use operator::Operator;
pub use operator::OperatorBuilder;
pub use operator::OperatorMetadata;

mod builder;
pub use builder::Builder;

mod error;
pub use error::Error;
pub use error::ErrorKind;
pub use error::Result;
