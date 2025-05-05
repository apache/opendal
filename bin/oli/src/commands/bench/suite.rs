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

use crate::commands::bench::report::{Report, SampleSet};
use crate::make_tokio_runtime;
use anyhow::{ensure, Context, Result};
use opendal::Operator;
use serde::{Deserialize, Deserializer};
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Deserialize, Debug)]
struct BenchSuiteConfig {
    /// Workload to run.
    workload: Workload,

    /// Number of parallel tasks to run.
    ///
    /// Default to 1.
    parallelism: Option<u32>,

    /// Size of file.
    #[serde(deserialize_with = "deserialize_file_size")]
    file_size: u64,

    /// Maximum time to run the bench suite.
    #[serde(with = "humantime_serde")]
    timeout: Duration,

    /// Whether retain the object on success.
    ///
    /// Default to false, which means the object used in the suite will be deleted
    /// after the suite successfully returned.
    #[serde(default)]
    retain_on_success: bool,
}

fn deserialize_file_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_size::parse_size(&s).map_err(serde::de::Error::custom)
}

#[derive(Deserialize, Debug)]
enum Workload {
    #[serde(rename = "upload")]
    Upload,
    #[serde(rename = "download")]
    Download,
}

pub struct BenchSuite {
    config: BenchSuiteConfig,
}

impl BenchSuite {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config = toml::from_str::<BenchSuiteConfig>(&content)?;
        ensure!(
            config.file_size >= 4096,
            "file_size must be greater or equal to 4096"
        );
        println!("Create bench suite with config: {config:?}");
        Ok(BenchSuite { config })
    }

    pub fn run(self, op: Operator) -> Result<()> {
        println!("Start running bench suite ...");
        let start = Instant::now();

        let timeout = self.config.timeout;
        let parallelism = self.config.parallelism.unwrap_or(1) as usize;
        let file_size = self.config.file_size;
        let workload = match self.config.workload {
            Workload::Upload => "upload".to_string(),
            Workload::Download => "download".to_string(),
        };
        let retain_on_success = self.config.retain_on_success;

        let rt = make_tokio_runtime(parallelism);

        let path = format!("obench-test-{}", uuid::Uuid::new_v4());
        println!("Prepare task with path: {path}");
        let task = rt
            .block_on(Task::prepare(&self.config, &path, &op))
            .context("failed to prepare task")?;

        let mut results = vec![];
        for _ in 0..parallelism {
            let op = op.clone();
            let task = task.clone();
            results.push(rt.spawn(async move {
                let mut bandwidth = SampleSet::default();
                let mut latency = SampleSet::default();
                let mut iops = SampleSet::default();
                let mut count = 0;

                loop {
                    if start.elapsed() > timeout {
                        return Ok::<_, anyhow::Error>((bandwidth, latency, iops));
                    }

                    let iter_start = Instant::now();
                    let iter_bytes = task.run(&op).await.context("failed to execute task")?;
                    let iter_latency = iter_start.elapsed();
                    count += 1;
                    latency.add(iter_latency.as_micros() as f64);
                    bandwidth.add(iter_bytes as f64 / iter_latency.as_secs_f64());
                    iops.add(count as f64 / start.elapsed().as_secs_f64());
                }
            }))
        }

        let mut bandwidth = SampleSet::default();
        let mut latency = SampleSet::default();
        let mut iops = SampleSet::default();

        for result in results {
            let (iter_bandwidth, iter_latency, iter_iops) = pollster::block_on(result)??;
            bandwidth.merge(iter_bandwidth);
            latency.merge(iter_latency);
            iops.merge(iter_iops);
        }

        if !retain_on_success {
            rt.block_on(async {
                println!("Deleting object at path: {path}");
                if let Err(err) = op.delete(&path).await {
                    eprintln!("failed to delete object: {}", err);
                }
            });
        }

        let report = Report::new(parallelism, file_size, workload, bandwidth, latency, iops);
        println!("Bench suite completed in {:?}; result:\n", start.elapsed());
        println!("{report}");
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum Task {
    Upload { path: String, file_size: u64 },
    Download { path: String },
}

const BATCH_SIZE: u64 = 4096;

impl Task {
    async fn prepare(config: &BenchSuiteConfig, path: &str, op: &Operator) -> Result<Task> {
        let path = path.to_string();
        let file_size = config.file_size;
        match config.workload {
            Workload::Upload => Ok(Task::Upload { path, file_size }),
            Workload::Download => {
                let mut writer = op.writer(&path).await?;
                let batch_cnt = file_size / (BATCH_SIZE);
                for _ in 0..batch_cnt {
                    writer.write(vec![139u8; BATCH_SIZE as usize]).await?
                }
                writer.close().await?;
                Ok(Task::Download { path })
            }
        }
    }

    async fn run(&self, op: &Operator) -> Result<u64> {
        match self {
            Task::Upload { path, file_size } => {
                let mut writer = op.writer(path).await?;
                for _ in 0..(*file_size / BATCH_SIZE) {
                    writer.write(vec![254u8; BATCH_SIZE as usize]).await?;
                }
                writer.close().await?;
                Ok((*file_size / BATCH_SIZE) * BATCH_SIZE)
            }
            Task::Download { path } => {
                let bytes = op.read_with(path).await?;
                Ok(bytes.len() as u64)
            }
        }
    }
}
