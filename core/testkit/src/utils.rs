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

use std::collections::HashMap;
use std::env;
use std::sync::LazyLock;

use opendal_core::Capability;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_core::layers::CapabilityOverrideLayer;
use opendal_layer_logging::LoggingLayer;
use opendal_layer_retry::RetryLayer;
use opendal_layer_timeout::TimeoutLayer;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

const OPENDAL_TEST_CAPABILITY_OVERRIDES: &str = "OPENDAL_TEST_CAPABILITY_OVERRIDES";

pub(crate) fn sha256_digest(data: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;

    let digest = Sha256::digest(data);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String must succeed");
    }
    output
}

/// TEST_RUNTIME is the runtime used for running tests.
pub static TEST_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// Init a service with given scheme.
///
/// - Load scheme from `OPENDAL_TEST`
/// - Construct a new Operator with given root.
/// - Else, returns a `None` to represent no valid config for operator.
pub fn init_test_service() -> Result<Option<Operator>> {
    let _ = dotenvy::dotenv();

    let scheme = if let Ok(v) = env::var("OPENDAL_TEST") {
        v
    } else {
        return Ok(None);
    };

    let prefix = {
        let scheme_key = scheme.replace('-', "_");
        format!("opendal_{scheme_key}_")
    };

    let mut cfg = env::vars()
        .filter_map(|(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect::<HashMap<String, String>>();

    // Use random root unless OPENDAL_DISABLE_RANDOM_ROOT is set to true.
    let disable_random_root = env::var("OPENDAL_DISABLE_RANDOM_ROOT").unwrap_or_default() == "true";
    if !disable_random_root {
        let root = format!(
            "{}{}/",
            cfg.get("root").cloned().unwrap_or_else(|| "/".to_string()),
            uuid::Uuid::new_v4()
        );
        cfg.insert("root".to_string(), root);
    }

    // string-based scheme uses a hyphen ('-') as the connector
    let scheme = scheme.replace('_', "-");
    let mut op = Operator::via_iter(scheme, cfg).expect("must succeed");

    if let Ok(overrides) = env::var(OPENDAL_TEST_CAPABILITY_OVERRIDES) {
        let overrides = CapabilityOverrides::parse(&overrides)?;
        op = op.layer(CapabilityOverrideLayer::new(move |cap| {
            overrides.apply(cap)
        }));
    }

    let op = op
        .layer(LoggingLayer::default())
        .layer(TimeoutLayer::new())
        .layer(RetryLayer::new().with_max_times(4));

    Ok(Some(op))
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
    if let Some(name) = token.strip_prefix('+') {
        return Ok((name.trim(), Value::Bool(true)));
    }

    if let Some(name) = token.strip_prefix('-') {
        return Ok((name.trim(), Value::Bool(false)));
    }

    let Some((name, value)) = token.split_once('=') else {
        return Err(invalid_capability_override(
            token,
            "expected `+capability`, `-capability`, or `capability=value`",
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
        format!("invalid {OPENDAL_TEST_CAPABILITY_OVERRIDES} entry `{token}`: {reason}"),
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

    #[test]
    fn parse_capability_overrides() {
        let overrides = CapabilityOverrides::parse("-read,+write_can_append,delete_max_size=7")
            .expect("override must parse");
        let cap = overrides.apply(Capability {
            read: true,
            delete_max_size: Some(1000),
            ..Default::default()
        });

        assert!(!cap.read);
        assert!(cap.write_can_append);
        assert_eq!(cap.delete_max_size, Some(7));
    }

    #[test]
    fn parse_bool_assignments_and_unset_sizes() {
        let overrides = CapabilityOverrides::parse("read=false,write=true,delete_max_size=none")
            .expect("override must parse");
        let cap = overrides.apply(Capability {
            read: true,
            delete_max_size: Some(1000),
            ..Default::default()
        });

        assert!(!cap.read);
        assert!(cap.write);
        assert_eq!(cap.delete_max_size, None);
    }

    #[test]
    fn reject_unknown_capability() {
        let err = CapabilityOverrides::parse("-not_a_capability").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reject_invalid_capability_type() {
        let err = CapabilityOverrides::parse("read=1").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}
