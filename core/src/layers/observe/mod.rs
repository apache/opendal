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

//! OpenDAL Observability Layer
//!
//! This module offers essential components to facilitate the implementation of observability in OpenDAL.

mod metrics;

pub use metrics::MetricMetadata;
pub use metrics::MetricsAccessor;
pub use metrics::MetricsIntercept;
pub use metrics::MetricsLayer;
pub use metrics::LABEL_ERROR;
pub use metrics::LABEL_NAMESPACE;
pub use metrics::LABEL_OPERATION;
pub use metrics::LABEL_PATH;
pub use metrics::LABEL_ROOT;
pub use metrics::LABEL_SCHEME;
pub use metrics::METRIC_OPERATION_BYTES;
pub use metrics::METRIC_OPERATION_DURATION_SECONDS;
pub use metrics::METRIC_OPERATION_ERRORS_TOTAL;

/// Return the path label value according to the given `path` and `level`.
///
/// - level = 0: return `None`, which means we ignore the path label.
/// - level > 0: the path label will be the path split by "/" and get the last n level,
///   if n=1 and input path is "abc/def/ghi", and then we'll use "abc/" as the path label.
pub fn path_label_value(path: &str, level: usize) -> Option<&str> {
    if path.is_empty() {
        return None;
    }

    if level > 0 {
        let label_value = path
            .char_indices()
            .filter(|&(_, c)| c == '/')
            .nth(level - 1)
            .map_or(path, |(i, _)| &path[..i]);
        Some(label_value)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_label_value() {
        let path = "abc/def/ghi";
        assert_eq!(path_label_value(path, 0), None);
        assert_eq!(path_label_value(path, 1), Some("abc"));
        assert_eq!(path_label_value(path, 2), Some("abc/def"));
        assert_eq!(path_label_value(path, 3), Some("abc/def/ghi"));
        assert_eq!(path_label_value(path, usize::MAX), Some("abc/def/ghi"));

        assert_eq!(path_label_value("", 1), None);
    }
}
