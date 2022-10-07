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

use super::KeyValueAccessor;
use crate::Accessor;
use crate::AccessorCapability;
use crate::AccessorMetadata;

/// Backend of kv service.
#[derive(Debug)]
pub struct Backend<S: KeyValueAccessor> {
    #[allow(dead_code)]
    kv: S,
}

impl<S> Accessor for Backend<S>
where
    S: KeyValueAccessor,
{
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_capabilities(
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
        );
        am
    }
}
