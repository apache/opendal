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

//! OpenDAL Observability
//!
//! This module offers essential components to facilitate the implementation of observability in OpenDAL.
//!
//! # OpenDAL Metrics Reference
//!
//! This document describes all metrics exposed by OpenDAL.
//!
//! ## Operation Metrics
//!
//! These metrics track operations at the storage abstraction level.
//!
//! | Metric Name                      | Type      | Description                                                                                | Labels                                          |
//! |----------------------------------|-----------|--------------------------------------------------------------------------------------------|-------------------------------------------------|
//! | operation_bytes                  | Histogram | Current operation size in bytes, represents the size of data being processed               | scheme, namespace, root, operation, path        |
//! | operation_bytes_rate             | Histogram | Histogram of data processing rates in bytes per second within individual operations        | scheme, namespace, root, operation, path        |
//! | operation_entries                | Histogram | Current operation size in entries, represents the entries being processed                  | scheme, namespace, root, operation, path        |
//! | operation_entries_rate           | Histogram | Histogram of entries processing rates in entries per second within individual operations   | scheme, namespace, root, operation, path        |
//! | operation_duration_seconds       | Histogram | Duration of operations in seconds, measured from start to completion                       | scheme, namespace, root, operation, path        |
//! | operation_errors_total           | Counter   | Total number of failed operations                                                         | scheme, namespace, root, operation, path, error |
//! | operation_executing              | Gauge     | Number of operations currently being executed                                             | scheme, namespace, root, operation              |
//! | operation_ttfb_seconds           | Histogram | Time to first byte in seconds for operations                                              | scheme, namespace, root, operation, path        |
//!
//! ## HTTP Metrics
//!
//! These metrics track the underlying HTTP requests made by OpenDAL services that use HTTP.
//!
//! | Metric Name                      | Type      | Description                                                                                | Labels                                          |
//! |----------------------------------|-----------|--------------------------------------------------------------------------------------------|-------------------------------------------------|
//! | http_connection_errors_total     | Counter   | Total number of HTTP requests that failed before receiving a response                      | scheme, namespace, root, operation, error       |
//! | http_status_errors_total         | Counter   | Total number of HTTP requests that received error status codes (non-2xx responses)         | scheme, namespace, root, operation, status      |
//! | http_executing                   | Gauge     | Number of HTTP requests currently in flight from this client                              | scheme, namespace, root                         |
//! | http_request_bytes               | Histogram | Histogram of HTTP request body sizes in bytes                                             | scheme, namespace, root, operation              |
//! | http_request_bytes_rate          | Histogram | Histogram of HTTP request bytes per second rates                                          | scheme, namespace, root, operation              |
//! | http_request_duration_seconds    | Histogram | Histogram of time spent sending HTTP requests, from first byte sent to first byte received | scheme, namespace, root, operation              |
//! | http_response_bytes              | Histogram | Histogram of HTTP response body sizes in bytes                                            | scheme, namespace, root, operation              |
//! | http_response_bytes_rate         | Histogram | Histogram of HTTP response bytes per second rates                                         | scheme, namespace, root, operation              |
//! | http_response_duration_seconds   | Histogram | Histogram of time spent receiving HTTP responses, from first byte to last byte received   | scheme, namespace, root, operation              |
//!
//! ## Label Descriptions
//!
//! | Label     | Description                                                   | Example Values                         |
//! |-----------|---------------------------------------------------------------|----------------------------------------|
//! | scheme    | The storage service scheme                                    | s3, gcs, azblob, fs, memory            |
//! | namespace | The storage service namespace (bucket, container, etc.)       | my-bucket, my-container                |
//! | root      | The root path within the namespace                            | /data, /backup                         |
//! | operation | The operation being performed                                 | read, write, stat, list, delete        |
//! | path      | The path of the object being operated on                      | /path/to/file.txt                      |
//! | error     | The error type or message for error metrics                   | not_found, permission_denied           |
//! | status    | The HTTP status code for HTTP error metrics                   | 404, 403, 500                          |
//!
//! ## Metric Types
//!
//! * **Histogram**: Distribution of values with configurable buckets, includes count, sum and quantiles
//! * **Counter**: Cumulative metric that only increases over time (resets on restart)
//! * **Gauge**: Point-in-time metric that can increase and decrease

mod metrics;

pub use metrics::MetricLabels;
pub use metrics::MetricValue;
pub use metrics::MetricsAccessor;
pub use metrics::MetricsIntercept;
pub use metrics::MetricsLayer;
pub use metrics::DEFAULT_BYTES_BUCKETS;
pub use metrics::DEFAULT_BYTES_RATE_BUCKETS;
pub use metrics::DEFAULT_DURATION_SECONDS_BUCKETS;
pub use metrics::DEFAULT_ENTRIES_BUCKETS;
pub use metrics::DEFAULT_ENTRIES_RATE_BUCKETS;
pub use metrics::DEFAULT_TTFB_BUCKETS;
pub use metrics::LABEL_ERROR;
pub use metrics::LABEL_NAMESPACE;
pub use metrics::LABEL_OPERATION;
pub use metrics::LABEL_ROOT;
pub use metrics::LABEL_SCHEME;
pub use metrics::LABEL_STATUS_CODE;
