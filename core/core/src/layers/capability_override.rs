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

use serde_json::Map;
use serde_json::Number;
use serde_json::Value;

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

    /// Create a new [`CapabilityOverrideLayer`] from capability override entries.
    ///
    /// The input is a comma-separated list of capability assignments:
    ///
    /// - `read=true` sets a boolean capability to `true`
    /// - `read=false` sets a boolean capability to `false`
    /// - `delete_max_size=1000` sets a numeric capability
    /// - `delete_max_size=none` unsets an optional capability
    pub fn from_overrides(input: &str) -> Result<Self> {
        let overrides = CapabilityOverrides::parse(input)?;
        Ok(Self::new(move |cap| overrides.apply(cap)))
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

#[derive(Clone, Debug, Default)]
struct CapabilityOverrides {
    values: Map<String, Value>,
}

impl CapabilityOverrides {
    fn parse(input: &str) -> Result<Self> {
        let mut overrides = Self::default();

        for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let (name, value) = parse_capability_override(token)?;
            overrides.values.insert(name.to_string(), value);
            overrides.try_apply(Capability::default()).map_err(|err| {
                invalid_capability_override(token, &format!("failed to apply override: {err}"))
            })?;
        }

        Ok(overrides)
    }

    fn apply(&self, cap: Capability) -> Capability {
        self.try_apply(cap)
            .expect("capability overrides must be validated before applying")
    }

    fn try_apply(&self, cap: Capability) -> Result<Capability> {
        let mut value = serde_json::to_value(cap).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to serialize capability: {err}"),
            )
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "serialized capability must be a JSON object",
            )
        })?;
        object.extend(self.values.clone());

        serde_json::from_value(value).map_err(|err| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("failed to deserialize capability overrides: {err}"),
            )
        })
    }
}

fn parse_capability_override(token: &str) -> Result<(&str, Value)> {
    let Some((name, value)) = token.split_once('=') else {
        return Err(invalid_capability_override(
            token,
            "expected `capability=value`",
        ));
    };

    Ok((
        name.trim(),
        parse_capability_value(value.trim())
            .map_err(|err| invalid_capability_override(token, &err.to_string()))?,
    ))
}

fn invalid_capability_override(token: &str, reason: &str) -> Error {
    Error::new(
        ErrorKind::ConfigInvalid,
        format!("invalid capability override entry `{token}`: {reason}"),
    )
}

fn parse_capability_value(value: &str) -> Result<Value> {
    match value {
        "true" | "on" | "yes" => Ok(Value::Bool(true)),
        "false" | "off" | "no" => Ok(Value::Bool(false)),
        "none" | "null" | "unset" => Ok(Value::Null),
        _ => value
            .parse::<usize>()
            .map(|v| Value::Number(Number::from(v)))
            .map_err(|_| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "expected a boolean, non-negative integer, or `none`",
                )
            }),
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

    #[test]
    fn parse_capability_overrides() -> Result<()> {
        let layer = CapabilityOverrideLayer::from_overrides(
            "read=false,write_can_append=true,delete_max_size=7",
        )?;
        let op = Operator::new(services::Memory::default())?
            .layer(layer)
            .finish();

        assert!(!op.info().full_capability().read);
        assert!(op.info().full_capability().write_can_append);
        assert_eq!(op.info().full_capability().delete_max_size, Some(7));

        Ok(())
    }

    #[test]
    fn parse_bool_assignments_and_unset_sizes() -> Result<()> {
        let layer =
            CapabilityOverrideLayer::from_overrides("read=false,write=true,delete_max_size=none")?;
        let op = Operator::new(services::Memory::default())?
            .layer(layer)
            .finish();

        assert!(!op.info().full_capability().read);
        assert!(op.info().full_capability().write);
        assert_eq!(op.info().full_capability().delete_max_size, None);

        Ok(())
    }

    #[test]
    fn reject_unknown_capability() {
        let err = CapabilityOverrideLayer::from_overrides("not_a_capability=false").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reject_capability_shorthand() {
        let err = CapabilityOverrideLayer::from_overrides("-read").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reject_invalid_capability_type() {
        let err = CapabilityOverrideLayer::from_overrides("read=1").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}
