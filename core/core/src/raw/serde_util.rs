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
use std::collections::hash_map::Entry;
use std::collections::hash_map::IntoIter;
use std::iter::empty;

use serde::de::Deserializer;
use serde::de::IntoDeserializer;
use serde::de::Visitor;
use serde::de::value::MapDeserializer;
use serde::de::value::SeqDeserializer;
use serde::de::{self};

use crate::*;

/// Parse xml serialize error into opendal::Error.
pub fn new_xml_serialize_error(e: quick_xml::SeError) -> Error {
    Error::new(ErrorKind::Unexpected, "serialize xml").set_source(e)
}

/// Parse xml deserialize error into opendal::Error.
pub fn new_xml_deserialize_error(e: quick_xml::DeError) -> Error {
    Error::new(ErrorKind::Unexpected, "deserialize xml").set_source(e)
}

/// Parse json serialize error into opendal::Error.
pub fn new_json_serialize_error(e: serde_json::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "serialize json").set_source(e)
}

/// Parse json deserialize error into opendal::Error.
pub fn new_json_deserialize_error(e: serde_json::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "deserialize json").set_source(e)
}

/// ConfigDeserializer is used to deserialize given configs from `HashMap<String, String>`.
///
/// This is only used by our services' config.
///
/// A map field takes a JSON object with unique keys and string values, such as
/// `{"fs.defaultFS":"hdfs://ns"}`. Keys inside the object keep their case,
/// unlike the config keys, which are lowercased.
pub struct ConfigDeserializer(MapDeserializer<'static, Pairs, de::value::Error>);

impl ConfigDeserializer {
    /// Create a new config deserializer.
    pub fn new(map: HashMap<String, String>) -> Self {
        let pairs = Pairs(map.into_iter());
        Self(MapDeserializer::new(pairs))
    }
}

impl<'de> Deserializer<'de> for ConfigDeserializer {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit seq
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any option newtype_struct enum
        struct
    }
}

/// Pairs is used to implement Iterator to meet the requirement of [`MapDeserializer`].
struct Pairs(IntoIter<String, String>);

impl Iterator for Pairs {
    type Item = (String, Pair);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.to_lowercase(), Pair(k, v)))
    }
}

/// Pair is used to hold both key and value of a config for better error output.
struct Pair(String, String);

impl IntoDeserializer<'_, de::value::Error> for Pair {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

impl<'de> Deserializer<'de> for Pair {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.1.into_deserializer().deserialize_any(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.to_lowercase().as_str() {
            "true" | "on" => true.into_deserializer().deserialize_bool(visitor),
            "false" | "off" => false.into_deserializer().deserialize_bool(visitor),
            _ => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, "invalid bool value"
            ))),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<i8>() {
            Ok(val) => val.into_deserializer().deserialize_i8(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<i16>() {
            Ok(val) => val.into_deserializer().deserialize_i16(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<i32>() {
            Ok(val) => val.into_deserializer().deserialize_i32(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<i64>() {
            Ok(val) => val.into_deserializer().deserialize_i64(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<u8>() {
            Ok(val) => val.into_deserializer().deserialize_u8(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<u16>() {
            Ok(val) => val.into_deserializer().deserialize_u16(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<u32>() {
            Ok(val) => val.into_deserializer().deserialize_u32(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<u64>() {
            Ok(val) => val.into_deserializer().deserialize_u64(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<f32>() {
            Ok(val) => val.into_deserializer().deserialize_f32(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.1.parse::<f64>() {
            Ok(val) => val.into_deserializer().deserialize_f64(visitor),
            Err(e) => Err(de::Error::custom(format_args!(
                "parse config '{}' with value '{}' failed for {:?}",
                self.0, self.1, e
            ))),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.1.is_empty() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Return empty instead of `[""]`.
        if self.1.is_empty() {
            SeqDeserializer::new(empty::<Pair>())
                .deserialize_seq(visitor)
                .map_err(|e| {
                    de::Error::custom(format_args!(
                        "parse config '{}' with value '{}' failed for {:?}",
                        self.0, self.1, e
                    ))
                })
        } else {
            let values = self
                .1
                .split(',')
                .map(|v| Pair(self.0.clone(), v.trim().to_owned()));
            SeqDeserializer::new(values)
                .deserialize_seq(visitor)
                .map_err(|e| {
                    de::Error::custom(format_args!(
                        "parse config '{}' with value '{}' failed for {:?}",
                        self.0, self.1, e
                    ))
                })
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Return empty for an empty value, as `deserialize_seq` does.
        if self.1.is_empty() {
            return MapDeserializer::new(empty::<(String, String)>()).deserialize_map(visitor);
        }

        // A JSON object keeps the case of its keys, and its values may contain
        // the `,` and `=` that a delimited encoding would need to escape.
        let mut json = serde_json::Deserializer::from_str(&self.1);
        let entries = (&mut json)
            .deserialize_map(UniqueStringMapVisitor)
            .and_then(|entries| json.end().map(|()| entries))
            .map_err(|e| {
                de::Error::custom(format_args!(
                    "parse config '{}' failed: expected a JSON object with unique keys and string values: {e}",
                    self.0
                ))
            })?;
        MapDeserializer::new(entries.into_iter()).deserialize_map(visitor)
    }

    serde::forward_to_deserialize_any! {
        char str string unit newtype_struct enum
        bytes byte_buf unit_struct tuple_struct
        identifier tuple ignored_any
        struct
    }
}

/// Collects a JSON object with string values and rejects a repeated key, which
/// the `HashMap` deserializer would silently overwrite.
struct UniqueStringMapVisitor;

impl<'de> Visitor<'de> for UniqueStringMapVisitor {
    type Value = HashMap<String, String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object with string values")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut entries = HashMap::new();
        while let Some((key, value)) = access.next_entry::<String, String>()? {
            match entries.entry(key) {
                Entry::Occupied(entry) => {
                    return Err(de::Error::custom(format_args!(
                        "duplicate key `{}`",
                        entry.key()
                    )));
                }
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Default, Deserialize, Eq, PartialEq)]
    #[serde(default)]
    #[non_exhaustive]
    pub struct TestConfig {
        bool_value: bool,
        bool_option_value_none: Option<bool>,
        bool_option_value_some: Option<bool>,
        bool_value_with_on: bool,
        bool_value_with_off: bool,

        string_value: String,
        string_option_value_none: Option<String>,
        string_option_value_some: Option<String>,

        u8_value: u8,
        u16_value: u16,
        u32_value: u32,
        u64_value: u64,
        i8_value: i8,
        i16_value: i16,
        i32_value: i32,
        i64_value: i64,

        vec_value: Vec<String>,
        vec_value_two: Vec<String>,
        vec_none: Option<Vec<String>>,
        vec_empty: Vec<String>,

        map_value: HashMap<String, String>,
        map_option_value_none: Option<HashMap<String, String>>,
        map_option_value_empty: Option<HashMap<String, String>>,
        map_empty: HashMap<String, String>,
    }

    #[test]
    fn test_config_deserializer() {
        let mut map = HashMap::new();
        map.insert("bool_value", "true");
        map.insert("bool_option_value_none", "");
        map.insert("bool_option_value_some", "false");
        map.insert("bool_value_with_on", "on");
        map.insert("bool_value_with_off", "off");
        map.insert("string_value", "hello");
        map.insert("string_option_value_none", "");
        map.insert("string_option_value_some", "hello");
        map.insert("u8_value", "8");
        map.insert("u16_value", "16");
        map.insert("u32_value", "32");
        map.insert("u64_value", "64");
        map.insert("i8_value", "-8");
        map.insert("i16_value", "16");
        map.insert("i32_value", "-32");
        map.insert("i64_value", "64");
        map.insert("vec_value", "hello");
        map.insert("vec_value_two", "hello,world");
        map.insert("vec_none", "");
        map.insert("vec_empty", "");
        map.insert(
            "map_value",
            r#"{"fs.defaultFS":"hdfs://ns","dfs.ha.namenodes.ns":"nn1,nn2"}"#,
        );
        map.insert("map_option_value_none", "");
        map.insert("map_option_value_empty", "{}");
        map.insert("map_empty", "");
        let map = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let output = TestConfig::deserialize(ConfigDeserializer::new(map)).unwrap();
        assert_eq!(
            output,
            TestConfig {
                bool_value: true,
                bool_option_value_none: None,
                bool_option_value_some: Some(false),
                bool_value_with_on: true,
                bool_value_with_off: false,
                string_value: "hello".to_string(),
                string_option_value_none: None,
                string_option_value_some: Some("hello".to_string()),
                u8_value: 8,
                u16_value: 16,
                u32_value: 32,
                u64_value: 64,
                i8_value: -8,
                i16_value: 16,
                i32_value: -32,
                i64_value: 64,
                vec_value: vec!["hello".to_string()],
                vec_value_two: vec!["hello".to_string(), "world".to_string()],
                vec_none: None,
                vec_empty: vec![],
                map_value: HashMap::from([
                    ("fs.defaultFS".to_string(), "hdfs://ns".to_string()),
                    ("dfs.ha.namenodes.ns".to_string(), "nn1,nn2".to_string()),
                ]),
                map_option_value_none: None,
                map_option_value_empty: Some(HashMap::new()),
                map_empty: HashMap::new(),
            }
        );
    }

    #[test]
    fn test_config_deserializer_rejects_invalid_map() {
        for value in [
            "fs.defaultFS=hdfs://ns",
            r#"["fs.defaultFS"]"#,
            r#"{"dfs.replication":3}"#,
            r#"{"fs.defaultFS":null}"#,
            r#"{"fs":{"defaultFS":"hdfs://ns"}}"#,
        ] {
            let map = HashMap::from([("map_value".to_string(), value.to_string())]);
            let err = TestConfig::deserialize(ConfigDeserializer::new(map)).unwrap_err();
            assert!(
                err.to_string()
                    .starts_with("parse config 'map_value' failed: expected a JSON object"),
                "{value}: {err}"
            );
        }
    }

    #[test]
    fn test_config_deserializer_rejects_duplicate_map_keys() {
        for value in [
            r#"{"fs.defaultFS":"hdfs://a","fs.defaultFS":"hdfs://b"}"#,
            r#"{"fs.defaultFS":"hdfs://a","fs.defaultFS":"hdfs://a"}"#,
        ] {
            let map = HashMap::from([("map_value".to_string(), value.to_string())]);
            let err = TestConfig::deserialize(ConfigDeserializer::new(map))
                .unwrap_err()
                .to_string();
            assert!(
                err.starts_with("parse config 'map_value' failed: expected a JSON object"),
                "{value}: {err}"
            );
            assert!(
                err.contains("duplicate key `fs.defaultFS`"),
                "{value}: {err}"
            );
        }
    }

    #[test]
    fn test_part_config_deserializer() {
        let mut map = HashMap::new();
        map.insert("bool_value", "true");
        let map = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let output = TestConfig::deserialize(ConfigDeserializer::new(map)).unwrap();
        assert_eq!(
            output,
            TestConfig {
                bool_value: true,
                ..TestConfig::default()
            }
        );
    }
}
