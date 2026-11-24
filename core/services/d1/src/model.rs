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

use std::fmt::Debug;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

use opendal_core::*;

/// response data from d1
#[derive(Deserialize, Debug)]
pub struct D1Response {
    #[serde(default)]
    pub result: Option<Vec<D1Result>>,
    pub success: bool,
    #[serde(default)]
    pub errors: Option<Vec<D1Error>>,
}

impl D1Response {
    pub fn parse(bs: &Bytes) -> Result<D1Response, Error> {
        let response: D1Response = serde_json::from_slice(bs).map_err(|e| {
            Error::new(
                opendal_core::ErrorKind::Unexpected,
                format!("failed to parse d1 response: {e}"),
            )
        })?;

        if !response.success {
            let (kind, retryable) = response
                .errors
                .as_deref()
                .and_then(crate::core::parse_d1_error_code)
                .unwrap_or((opendal_core::ErrorKind::Unexpected, false));
            let mut err = Error::new(kind, String::from_utf8_lossy(bs));
            if retryable {
                err = err.set_temporary();
            }
            return Err(err);
        }
        Ok(response)
    }

    pub fn get_result(&self, key: &str) -> Result<Option<Buffer>, Error> {
        let Some(first_result) = self.result.as_deref().and_then(|r| r.first()) else {
            return Ok(None);
        };
        let Some(row) = first_result.results.first() else {
            return Ok(None);
        };
        let Some(value) = row.get(key) else {
            return Ok(None);
        };

        match value {
            Value::Array(items) => {
                let mut bytes = Vec::with_capacity(items.len());
                for item in items {
                    let byte = item
                        .as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(|| {
                            Error::new(
                                opendal_core::ErrorKind::Unexpected,
                                "d1 value contains invalid byte element",
                            )
                        })?;
                    bytes.push(byte);
                }
                Ok(Some(Buffer::from(bytes)))
            }
            Value::Null => Ok(None),
            _ => Err(Error::new(
                opendal_core::ErrorKind::Unexpected,
                "d1 value is not a byte array",
            )),
        }
    }

    pub fn get_usize_result(&self, key: &str) -> Result<Option<usize>, Error> {
        let Some(first_result) = self.result.as_deref().and_then(|r| r.first()) else {
            return Ok(None);
        };
        let Some(row) = first_result.results.first() else {
            return Ok(None);
        };
        let Some(value) = row.get(key) else {
            return Ok(None);
        };

        match value {
            Value::Number(n) => {
                let value = n.as_u64().ok_or_else(|| {
                    Error::new(
                        opendal_core::ErrorKind::Unexpected,
                        "d1 value length is invalid",
                    )
                })?;
                value.try_into().map(Some).map_err(|err| {
                    Error::new(
                        opendal_core::ErrorKind::Unexpected,
                        "d1 value length is invalid",
                    )
                    .set_source(err)
                })
            }
            _ => Err(Error::new(
                opendal_core::ErrorKind::Unexpected,
                "d1 value length is invalid",
            )),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct D1Result {
    #[serde(default)]
    pub results: Vec<Map<String, Value>>,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct D1Error {
    pub message: String,
    pub code: i32,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_get_object_json_response() {
        let data = r#"
        {
            "result": [
                {
                    "results": [
                        {
                            "CustomerId": "4",
                            "CompanyName": "Around the Horn",
                            "ContactName": "Thomas Hardy"
                        }
                    ],
                    "success": true,
                    "meta": {
                        "served_by": "v3-prod",
                        "duration": 0.2147,
                        "changes": 0,
                        "last_row_id": 0,
                        "changed_db": false,
                        "size_after": 2162688,
                        "rows_read": 3,
                        "rows_written": 2
                    }
                }
            ],
            "success": true,
            "errors": [],
            "messages": []
        }"#;
        let response: D1Response = serde_json::from_str(data).unwrap();
        println!("{:?}", response.result.as_ref().unwrap()[0].results[0]);
    }

    #[test]
    fn test_get_usize_result() {
        let data = r#"
        {
            "result": [
                {
                    "results": [
                        {
                            "content_length": 6
                        }
                    ],
                    "success": true,
                    "meta": {}
                }
            ],
            "success": true,
            "errors": [],
            "messages": []
        }"#;

        let response: D1Response = serde_json::from_str(data).unwrap();
        assert_eq!(
            response.get_usize_result("content_length").unwrap(),
            Some(6)
        );
        assert_eq!(response.get_usize_result("missing").unwrap(), None);
    }

    #[test]
    fn test_get_result_validates_byte_elements() {
        let ok_data = r#"{
            "result": [{"results": [{"value": [65, 66]}]}],
            "success": true,
            "errors": []
        }"#;
        let response: D1Response = serde_json::from_str(ok_data).unwrap();
        let buf = response.get_result("value").unwrap().unwrap();
        assert_eq!(buf.to_bytes().as_ref(), b"AB");

        let empty_data = r#"{
            "result": [{"results": [{"value": []}]}],
            "success": true,
            "errors": []
        }"#;
        let response: D1Response = serde_json::from_str(empty_data).unwrap();
        let buf = response.get_result("value").unwrap().unwrap();
        assert!(buf.is_empty());

        let null_data = r#"{
            "result": [{"results": [{"value": null}]}],
            "success": true,
            "errors": []
        }"#;
        let response: D1Response = serde_json::from_str(null_data).unwrap();
        assert!(response.get_result("value").unwrap().is_none());

        for invalid_value in ["[-1]", "[1.5]", "[256]", r#"["x"]"#, r#""not-an-array""#] {
            let data = format!(
                r#"{{"result":[{{"results":[{{"value":{invalid_value}}}]}}],"success":true,"errors":[]}}"#
            );
            let response: D1Response = serde_json::from_str(&data).unwrap();
            let err = response.get_result("value").unwrap_err();
            assert_eq!(
                err.kind(),
                ErrorKind::Unexpected,
                "expected Unexpected for {invalid_value}"
            );
        }
    }

    #[test]
    fn test_parse_error_response_with_null_or_missing_result() {
        let null_result = Bytes::from_static(
            br#"{"success":false,"result":null,"errors":[{"code":10000,"message":"Authentication error"}]}"#,
        );
        let err = D1Response::parse(&null_result).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);

        let missing_result = Bytes::from_static(
            br#"{"success":false,"errors":[{"code":7500,"message":"no such column"}]}"#,
        );
        let err = D1Response::parse(&missing_result).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }
}
