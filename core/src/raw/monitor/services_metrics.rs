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

use prometheus::{
    core::{AtomicU64, GenericCounter},
    register_int_counter_with_registry, Histogram,
};
use prometheus::{exponential_buckets, histogram_opts, register_histogram_with_registry, Registry};

/// [`AccessorBasicMetrics`] stores the performance and IO metrics of AccessorBasicMetrics.
#[derive(Debug)]
pub struct AccessorBasicMetrics {
    /// Histogram of the time spent on accessor listing
    pub list_duration: Histogram,
    /// Total number of records that have been listed
    pub list_counts: GenericCounter<AtomicU64>,

    /// Histogram of the time spent on accessor scaning
    pub scan_duration: Histogram,
}

impl AccessorBasicMetrics {
    /// new with prometheus register.
    pub fn new(registry: Registry) -> Self {
        // ----- list -----
        let opts = histogram_opts!(
            "accessor_list_duration",
            "Histogram of the time spent on accessor listing",
            exponential_buckets(0.01, 2.0, 16).unwrap() // need to estimate an approximate maximum time.
        );

        let list_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let list_counts = register_int_counter_with_registry!(
            "accessor_list_counts",
            "Total number of records that have been listed",
            registry
        )
        .unwrap();

        // ----- scan -----
        let opts = histogram_opts!(
            "accessor_scan_duration",
            "Histogram of the time spent on accessor scaning",
            exponential_buckets(0.01, 2.0, 16).unwrap() // // need to estimate an approximate maximum time.
        );

        let scan_duration = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            list_duration,
            list_counts,
            scan_duration,
        }
    }

    /// Creates a new `AccessorBasicMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
