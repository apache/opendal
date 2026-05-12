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

use std::fmt;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

/// Layer for overriding an accessor's full capability.
///
/// This layer updates [`Capability`] exposed by
/// [`OperatorInfo::full_capability`][crate::OperatorInfo::full_capability]
/// without changing the accessor's native capability. It is useful when the
/// backend implementation supports a capability, but the specific endpoint or
/// test setup needs to disable or tune it.
///
/// # Examples
///
/// ```no_run
/// use opendal_core::layers::CapabilityOverrideLayer;
/// use opendal_core::services;
/// use opendal_core::Operator;
/// use opendal_core::Result;
///
/// # fn main() -> Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     .layer(CapabilityOverrideLayer::new(|mut cap| {
///         cap.write_with_if_match = false;
///         cap.delete_max_size = Some(700);
///         cap
///     }))
///     .finish();
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CapabilityOverrideLayer {
    apply: Arc<dyn Fn(Capability) -> Capability + Send + Sync>,
}

impl fmt::Debug for CapabilityOverrideLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityOverrideLayer")
            .finish_non_exhaustive()
    }
}

impl CapabilityOverrideLayer {
    /// Create a new [`CapabilityOverrideLayer`].
    pub fn new(apply: impl Fn(Capability) -> Capability + Send + Sync + 'static) -> Self {
        Self {
            apply: Arc::new(apply),
        }
    }
}

impl<A: Access> Layer<A> for CapabilityOverrideLayer {
    type LayeredAccess = A;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        let apply = self.apply.clone();
        info.update_full_capability(|cap| apply(cap));
        inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operator;
    use crate::services;

    #[test]
    fn capability_override_updates_full_capability_only() -> Result<()> {
        let op = Operator::new(services::Memory::default())?
            .layer(CapabilityOverrideLayer::new(|mut cap| {
                cap.read = false;
                cap.delete_max_size = Some(7);
                cap
            }))
            .finish();

        assert!(!op.info().full_capability().read);
        assert_eq!(op.info().full_capability().delete_max_size, Some(7));

        assert!(op.info().native_capability().read);
        assert_ne!(
            op.info().native_capability().delete_max_size,
            op.info().full_capability().delete_max_size
        );

        Ok(())
    }
}
