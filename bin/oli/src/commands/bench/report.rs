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

use std::fmt::{Display, Formatter};
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct Report {
    // bench suite infos
    parallelism: u32,
    file_size: u32,
    workload: String,

    // bench result metrics
    /// Throughput (bytes per second).
    bandwidth: Metric,
    /// Latency (microseconds).
    latency: Metric,
    /// IOPS (operations per second).
    iops: Metric,
}

impl Report {
    pub fn new(
        parallelism: u32,
        file_size: u32,
        workload: String,
        bandwidth: SampleSet,
        latency: SampleSet,
        iops: SampleSet,
    ) -> Self {
        Self {
            parallelism,
            file_size,
            workload,
            bandwidth: bandwidth.to_metric(),
            latency: latency.to_metric(),
            iops: iops.to_metric(),
        }
    }
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Parallel tasks: {}", self.parallelism)?;
        writeln!(f, "Workload: {}", self.workload)?;
        writeln!(
            f,
            "File size: {}",
            humansize::format_size(self.file_size, humansize::BINARY)
        )?;

        writeln!(f)?;
        writeln!(f, "Bandwidth:")?;
        writeln!(
            f,
            "{}",
            self.bandwidth.format(2, |x| {
                format!("{}/s", humansize::format_size_i(x, humansize::BINARY))
            })
        )?;

        writeln!(f)?;
        writeln!(f, "Latency:")?;
        writeln!(
            f,
            "{}",
            self.latency.format(2, |x| {
                let dur = Duration::from_micros(x as u64);
                format!("{}", humantime::format_duration(dur))
            })
        )?;

        writeln!(f)?;
        writeln!(f, "IOPS:")?;
        writeln!(f, "{}", self.iops.format(2, |x| { format!("{x:.3}") }))?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Metric {
    num_samples: u32,
    min: f64,
    max: f64,
    avg: f64,
    stddev: f64,
    p99: f64,
    p95: f64,
    p50: f64,
}

impl Metric {
    fn format(&self, indent: usize, formatter: fn(f64) -> String) -> String {
        format!(
            "{:indent$}num_samples: {}\n\
             {:indent$}min: {}\n\
             {:indent$}max: {}\n\
             {:indent$}avg: {}\n\
             {:indent$}stddev: {}\n\
             {:indent$}p99: {}\n\
             {:indent$}p95: {}\n\
             {:indent$}p50: {}",
            "",
            self.num_samples,
            "",
            formatter(self.min),
            "",
            formatter(self.max),
            "",
            formatter(self.avg),
            "",
            formatter(self.stddev),
            "",
            formatter(self.p99),
            "",
            formatter(self.p95),
            "",
            formatter(self.p50),
        )
    }
}

#[derive(Debug, Default)]
pub(crate) struct SampleSet {
    values: Vec<f64>,
}

impl SampleSet {
    /// Add a new sample value.
    pub fn add(&mut self, sample: f64) {
        assert!(sample.is_finite(), "sample value must be finite");
        self.values.push(sample);
    }

    /// Merge two sample sets.
    pub fn merge(&mut self, other: SampleSet) {
        self.values.extend(other.values);
    }

    /// Get the minimum value.
    fn min(&self) -> Option<f64> {
        self.values.iter().copied().min_by(|a, b| a.total_cmp(b))
    }

    /// Get the maximum value.
    fn max(&self) -> Option<f64> {
        self.values.iter().copied().max_by(|a, b| a.total_cmp(b))
    }

    /// Get number of samples.
    fn count(&self) -> usize {
        self.values.len()
    }

    /// Get the average of values.
    fn avg(&self) -> Option<f64> {
        let count = self.count();
        if count == 0 {
            return None;
        }

        let sum: f64 = self.values.iter().copied().sum();
        Some(sum / (count as f64))
    }

    /// Get the standard deviation of values.
    fn stddev(&self) -> Option<f64> {
        let count = self.count();
        if count == 0 {
            return None;
        }

        let avg = self.avg()?;
        let sum = self
            .values
            .iter()
            .copied()
            .map(|x| (x - avg).powi(2))
            .sum::<f64>();
        Some((sum / count as f64).sqrt())
    }

    /// Get the percentile value.
    ///
    /// The percentile value must between 0.0 and 100.0 (both inclusive).
    fn percentile(&self, percentile: f64) -> Option<f64> {
        assert!(
            (0.0..=100.0).contains(&percentile),
            "percentile must be between 0.0 and 100.0"
        );

        let count = self.count();
        if count == 0 {
            return None;
        }

        let index = ((count - 1) as f64 * percentile / 100.0).trunc() as usize;
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        sorted.get(index).copied()
    }

    /// Create a metric from the sample set.
    fn to_metric(&self) -> Metric {
        Metric {
            num_samples: self.count() as u32,
            min: self.min().unwrap_or(f64::NAN),
            max: self.max().unwrap_or(f64::NAN),
            avg: self.avg().unwrap_or(f64::NAN),
            stddev: self.stddev().unwrap_or(f64::NAN),
            p99: self.percentile(99.0).unwrap_or(f64::NAN),
            p95: self.percentile(95.0).unwrap_or(f64::NAN),
            p50: self.percentile(50.0).unwrap_or(f64::NAN),
        }
    }
}
