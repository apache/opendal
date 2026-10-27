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

//! Compare OpenDAL read, write, and mixed workloads on an existing 3FS mount.
#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("The 3FS benchmark requires Linux.");
}

#[cfg(target_os = "linux")]
#[tokio::main(worker_threads = 8)]
async fn main() -> opendal_core::Result<()> {
    bench::run().await
}

#[cfg(target_os = "linux")]
mod bench {
    use std::ffi::CString;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use futures::TryStreamExt;
    use libloading::os::unix::Library;
    use opendal_core::{Buffer, Operator, Result};
    use opendal_service_hf3fs::Hf3fs;
    use tokio::sync::Barrier;

    const FILE_SIZE: usize = 1024 * 1024 * 1024;
    const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

    #[derive(Default)]
    struct Samples {
        reads: Vec<f64>,
        writes: Vec<f64>,
        closes: Vec<f64>,
    }

    #[derive(Clone, Copy)]
    enum Workload {
        Read,
        RandomRead,
        StreamWrite,
        Overwrite,
        Mixed70,
        Mixed50,
    }

    impl Workload {
        fn name(self) -> &'static str {
            match self {
                Self::Read => "seqread",
                Self::RandomRead => "randread",
                Self::StreamWrite => "seqwrite",
                Self::Overwrite => "overwrite",
                Self::Mixed70 => "mixed70",
                Self::Mixed50 => "mixed50",
            }
        }

        fn read_percent(self) -> usize {
            match self {
                Self::Read | Self::RandomRead => 100,
                Self::StreamWrite | Self::Overwrite => 0,
                Self::Mixed70 => 70,
                Self::Mixed50 => 50,
            }
        }
    }

    fn setting<T: std::str::FromStr>(name: &str, default: T) -> T {
        std::env::var(name).map_or(default, |v| v.parse().ok().expect(name))
    }

    fn percentiles(values: &mut [f64]) -> [f64; 3] {
        if values.is_empty() {
            return [0.0; 3];
        }
        values.sort_unstable_by(f64::total_cmp);
        [50, 95, 99].map(|p| values[((values.len() * p).div_ceil(100) - 1).min(values.len() - 1)])
    }

    fn cpu_seconds() -> f64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // getrusage initializes the output on success and does not retain it.
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(result, 0);
        let usage = unsafe { usage.assume_init() };
        (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
    }

    pub async fn run() -> Result<()> {
        let root = std::env::var("OPENDAL_HF3FS_ROOT").expect("set OPENDAL_HF3FS_ROOT");
        let directory = tempfile::tempdir_in(root).expect("create benchmark directory");
        let root = std::fs::canonicalize(directory.path()).expect("canonical benchmark root");
        let root = root.to_str().unwrap();
        let cache = std::env::var("BENCH_CACHE").unwrap_or("direct".into());
        assert!(matches!(cache.as_str(), "direct" | "buffered" | "hot"));
        // The preload helper exports this probe and checks the covered directory.
        let direct = unsafe {
            Library::this()
                .get::<unsafe extern "C" fn(*const libc::c_char) -> libc::c_int>(
                    b"opendal_bench_direct_for\0",
                )
                .is_ok_and(|probe| probe(CString::new(root).unwrap().as_ptr()) != 0)
        };
        assert_eq!(
            cache == "direct",
            direct,
            "BENCH_CACHE=direct requires bench_direct.so covering OPENDAL_HF3FS_ROOT; buffered/hot require no preload"
        );
        let fs = Operator::new(opendal_service_fs::Fs::default().root(root))?;
        let seconds = setting("BENCH_SECONDS", 2.0_f64);
        let rounds = setting("BENCH_ROUNDS", 4_usize);
        let warm_reads = cache == "hot";
        assert!(seconds.is_finite() && seconds > 0.0 && rounds > 0 && rounds.is_multiple_of(2));
        let duration = Duration::from_secs_f64(seconds);
        let block_sizes = std::env::var("BENCH_BS").unwrap_or("4096,65536,1048576,4194304".into());
        let concurrencies = std::env::var("BENCH_CONCURRENCY").unwrap_or("1,4,16,64".into());
        let selected = std::env::var("BENCH_WORKLOADS")
            .unwrap_or("seqread,randread,seqwrite,overwrite,mixed70,mixed50".into());
        let payload: Buffer = vec![0x5a; 4 * 1024 * 1024].into();
        let mut writer = fs.writer("input").await?;
        for _ in 0..FILE_SIZE / payload.len() {
            writer.write(payload.clone()).await?;
        }
        writer.close().await?;
        eprintln!(
            "file_bytes={FILE_SIZE} seconds={seconds} rounds={rounds} runtime_threads=8 queue_depth=64; cache={cache}; each service has one Operator; native chunks are at most 1 MiB; seqwrite latency measures write() admission; close/fsync is included in elapsed time"
        );
        println!(
            "cache,round,service,workload,bs,io_bs,concurrency,elapsed_s,read_ops,write_ops,read_mib_s,write_mib_s,iops,read_p50_us,read_p95_us,read_p99_us,write_p50_us,write_p95_us,write_p99_us,close_count,close_p50_us,close_p95_us,close_p99_us,cpu_cores"
        );
        for round in 0..rounds {
            for size in block_sizes.split(',') {
                let bs: usize = size.parse().expect("BENCH_BS");
                assert!(
                    bs >= 4096
                        && bs.is_multiple_of(4096)
                        && bs <= payload.len()
                        && FILE_SIZE.is_multiple_of(bs)
                );
                let io_bs = bs.min(1024 * 1024);
                let mut builder = Hf3fs::default()
                    .root(root)
                    .queue_depth(64)
                    .block_size(io_bs);
                if let Ok(path) = std::env::var("OPENDAL_HF3FS_LIBRARY_PATH") {
                    builder = builder.library_path(&path);
                }
                let hf3fs = Operator::new(builder)?;
                for concurrency in concurrencies.split(',') {
                    let concurrency: usize = concurrency.parse().expect("BENCH_CONCURRENCY");
                    assert!(concurrency > 0 && concurrency <= 64);
                    for workload in [
                        Workload::Read,
                        Workload::RandomRead,
                        Workload::StreamWrite,
                        Workload::Overwrite,
                        Workload::Mixed70,
                        Workload::Mixed50,
                    ] {
                        if !selected.split(',').any(|v| v == workload.name()) {
                            continue;
                        }
                        let order = if round % 2 == 0 {
                            [("fs", &fs), ("hf3fs", &hf3fs)]
                        } else {
                            [("hf3fs", &hf3fs), ("fs", &fs)]
                        };
                        for (name, operator) in order {
                            let barrier = Arc::new(Barrier::new(concurrency + 1));
                            let mut tasks = Vec::new();
                            let mut warm_reader = None;
                            for worker in 0..concurrency {
                                let operator = operator.clone();
                                let barrier = barrier.clone();
                                let payload = payload.slice(..bs);
                                let reader = operator
                                    .reader_with("input")
                                    .chunk(io_bs)
                                    .concurrent(1)
                                    .await?;
                                reader.read(0..bs as u64).await?;
                                warm_reader = Some(reader.clone());
                                let path = format!("output-{worker}");
                                let mut writer = operator
                                    .writer_with(&path)
                                    .chunk(io_bs)
                                    .concurrent(1)
                                    .await?;
                                tasks.push(tokio::spawn(async move {
                                    let mut samples = Samples::default();
                                    let mut rng = worker as u64 + 1;
                                    let mut sequence = worker * (FILE_SIZE / bs / concurrency);
                                    let mut written = 0;
                                    barrier.wait().await;
                                    let deadline = Instant::now() + duration;
                                    while Instant::now() < deadline {
                                        let read =
                                            (sequence + worker) % 10 < workload.read_percent() / 10;
                                        let start = Instant::now();
                                        if read {
                                            rng ^= rng << 13;
                                            rng ^= rng >> 7;
                                            rng ^= rng << 17;
                                            let block = if matches!(workload, Workload::Read) {
                                                sequence
                                            } else {
                                                rng as usize
                                            } % (FILE_SIZE / bs);
                                            let offset = (block * bs) as u64;
                                            let buffer =
                                                reader.read(offset..offset + bs as u64).await?;
                                            assert_eq!(buffer.len(), bs);
                                            assert_eq!(buffer.clone().next().unwrap()[0], 0x5a);
                                            std::hint::black_box(buffer);
                                            samples.reads.push(start.elapsed().as_secs_f64() * 1e6);
                                        } else if matches!(workload, Workload::StreamWrite) {
                                            writer.write(payload.clone()).await?;
                                            samples
                                                .writes
                                                .push(start.elapsed().as_secs_f64() * 1e6);
                                            written += bs;
                                            if written >= SEGMENT_SIZE {
                                                let start = Instant::now();
                                                writer.close().await?;
                                                samples
                                                    .closes
                                                    .push(start.elapsed().as_secs_f64() * 1e6);
                                                writer = operator
                                                    .writer_with(&path)
                                                    .chunk(io_bs)
                                                    .concurrent(1)
                                                    .await?;
                                                written = 0;
                                            }
                                        } else {
                                            operator
                                                .write_with(&path, payload.clone())
                                                .chunk(io_bs)
                                                .concurrent(1)
                                                .await?;
                                            samples
                                                .writes
                                                .push(start.elapsed().as_secs_f64() * 1e6);
                                        }
                                        sequence += 1;
                                    }
                                    if written > 0 {
                                        let start = Instant::now();
                                        writer.close().await?;
                                        samples.closes.push(start.elapsed().as_secs_f64() * 1e6);
                                    }
                                    Ok::<_, opendal_core::Error>(samples)
                                }));
                            }
                            if warm_reads && name == "fs" && workload.read_percent() > 0 {
                                // Reopening a FUSE file can invalidate its page cache. Warm
                                // through an existing handle after all workers have opened it.
                                let mut stream = warm_reader.unwrap().into_stream(..).await?;
                                while let Some(buffer) = stream.try_next().await? {
                                    std::hint::black_box(buffer);
                                }
                            }
                            let cpu = cpu_seconds();
                            let start = Instant::now();
                            barrier.wait().await;
                            let mut results = Vec::new();
                            for task in tasks {
                                results.push(task.await.expect("benchmark worker panicked")?);
                            }
                            let elapsed = start.elapsed().as_secs_f64();
                            let cpu = (cpu_seconds() - cpu) / elapsed;
                            let mut samples = Samples::default();
                            for sample in results {
                                samples.reads.extend(sample.reads);
                                samples.writes.extend(sample.writes);
                                samples.closes.extend(sample.closes);
                            }
                            let r = samples.reads.len();
                            let w = samples.writes.len();
                            let close_count = samples.closes.len();
                            let [r50, r95, r99] = percentiles(&mut samples.reads);
                            let [w50, w95, w99] = percentiles(&mut samples.writes);
                            let [c50, c95, c99] = percentiles(&mut samples.closes);
                            println!(
                                "{cache},{round},{name},{},{bs},{io_bs},{concurrency},{elapsed:.6},{r},{w},{:.3},{:.3},{:.3},{r50:.3},{r95:.3},{r99:.3},{w50:.3},{w95:.3},{w99:.3},{close_count},{c50:.3},{c95:.3},{c99:.3},{cpu:.3}",
                                workload.name(),
                                r as f64 * bs as f64 / elapsed / 1048576.0,
                                w as f64 * bs as f64 / elapsed / 1048576.0,
                                (r + w) as f64 / elapsed
                            );
                            if w > 0 {
                                for worker in 0..concurrency {
                                    let path = format!("output-{worker}");
                                    let buffer = operator
                                        .read_with(&path)
                                        .range(0..bs.min(4096) as u64)
                                        .await?;
                                    assert!(buffer.to_bytes().iter().all(|v| *v == 0x5a));
                                    operator.delete(&path).await?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
