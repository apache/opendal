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

//! Shared fuzz input generation for the fuzz targets.
//!
//! This crate intentionally does not depend on `libfuzzer-sys` so that its
//! unit tests can run with the standard test harness.

use arbitrary::Arbitrary;
use arbitrary::Unstructured;
use opendal::Operator;
use opendal::Result;
use opendal::tests::WriteAction;
use opendal::tests::WriteChecker;

/// Upper bound of the total bytes one `FuzzWriterInput` may write across all
/// its write actions.
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;
/// Upper bound of the total bytes produced when an explicit chunk size is
/// set, expressed as the number of chunks. A tiny `buffer` must not turn the
/// byte budget into millions of underlying writes (OSS-Fuzz issue
/// 530185493, apache/opendal#7848).
const MAX_CHUNKS: usize = 256;

/// Structured input for the `fuzz_writer` target.
#[derive(Debug, Clone)]
pub struct FuzzWriterInput {
    /// Write actions to perform, in order.
    pub actions: Vec<WriteAction>,
    /// Optional chunk size passed to `Writer::chunk`.
    pub buffer: Option<usize>,
    /// Optional concurrency passed to `Writer::concurrent`.
    pub concurrent: Option<usize>,
}

impl Arbitrary<'_> for FuzzWriterInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let mut actions = vec![];
        let buffer = if u.int_in_range(0..=1)? == 1 {
            Some(u.int_in_range(1..=8 * 1024 * 1024)?)
        } else {
            None
        };
        let concurrent = if u.int_in_range(0..=1)? == 1 {
            Some(u.int_in_range(0..=16)?)
        } else {
            None
        };

        // Each action produces at least one underlying write of `buffer`
        // bytes, so an explicit chunk size additionally caps the total
        // bytes at `buffer * MAX_CHUNKS`.
        let mut remaining = buffer.map_or(MAX_TOTAL_BYTES, |chunk: usize| {
            chunk.saturating_mul(MAX_CHUNKS).min(MAX_TOTAL_BYTES)
        });

        let count = u.int_in_range(1..=256)?;

        for _ in 0..count {
            if remaining == 0 {
                break;
            }
            let size = u.int_in_range(1..=remaining)?;
            remaining -= size;
            actions.push(WriteAction::Write(size));
        }

        Ok(FuzzWriterInput {
            actions,
            buffer,
            concurrent,
        })
    }
}

/// Write the generated actions through `op`'s writer, then read the object
/// back and verify its content.
pub async fn fuzz_writer(op: Operator, input: FuzzWriterInput) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let total_size = input
        .actions
        .iter()
        .map(|a| match a {
            WriteAction::Write(size) => *size,
        })
        .collect();

    let checker = WriteChecker::new(total_size);

    let mut writer = op.writer_with(&path);
    if let Some(buffer) = input.buffer {
        writer = writer.chunk(buffer);
    } else if let Some(min_size) = op.info().capability().write_multi_min_size {
        writer = writer.chunk(min_size);
    }
    if let Some(concurrent) = input.concurrent {
        writer = writer.concurrent(concurrent);
    }

    let mut writer = writer.await?;

    for chunk in checker.chunks() {
        writer.write(chunk.clone()).await?;
    }

    writer.close().await?;

    let result = op.read(&path).await?.to_bytes();

    checker.check(&result);

    op.delete(&path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::tests::TEST_RUNTIME;

    fn test_operator() -> Operator {
        let root = std::env::temp_dir().join(format!("opendal-fuzz-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root dir must succeed");
        Operator::new(opendal::services::Fs::default().root(&root.to_string_lossy()))
            .expect("operator init must succeed")
    }

    fn parse(input: &[u8]) -> FuzzWriterInput {
        FuzzWriterInput::arbitrary(&mut Unstructured::new(input)).expect("input must parse")
    }

    fn total_bytes(input: &FuzzWriterInput) -> usize {
        input
            .actions
            .iter()
            .map(|a| match a {
                WriteAction::Write(size) => *size,
            })
            .sum()
    }

    /// Regression test for OSS-Fuzz issue 530185493 (apache/opendal#7848):
    /// `buffer == 1` must cap the total written bytes at `MAX_CHUNKS`
    /// instead of generating millions of one-byte writes.
    #[test]
    fn chunk_size_one_respects_chunk_budget() {
        // buffer flag = 1, buffer = 1, concurrent flag = 0, count = 6,
        // then sizes 101, 101, 53 which exhaust the 256-byte budget.
        let input = parse(&[0x01, 0, 0, 0, 0x00, 0x05, 100, 100, 52]);
        assert_eq!(input.buffer, Some(1));
        assert!(total_bytes(&input) <= MAX_CHUNKS);

        TEST_RUNTIME
            .block_on(fuzz_writer(test_operator(), input))
            .expect("fuzz writer must succeed");
    }

    /// A chunk-size-one input must not generate more writes than its byte
    /// budget even when the requested action count is large.
    #[test]
    fn chunk_size_one_truncates_excess_actions() {
        // buffer = 1, no concurrent, count = 256: the first action takes
        // all 255 bytes it may, the second takes the last byte, and the
        // remaining 254 requested actions are dropped.
        let input = parse(&[0x01, 0, 0, 0, 0x00, 0xFF, 0xFE]);
        assert_eq!(input.buffer, Some(1));
        assert_eq!(input.actions.len(), 2);
        assert!(total_bytes(&input) <= MAX_CHUNKS);
    }

    /// Without an explicit chunk size, actions are still bounded by
    /// `MAX_TOTAL_BYTES` in total.
    #[test]
    fn total_bytes_respects_budget_without_chunk() {
        // No buffer, no concurrent, count = 4, the first action takes
        // 4 MiB - 1 and the second takes the last byte.
        let input = parse(&[0x00, 0x00, 0x03, 0x3F, 0xFF, 0xFE, 0xFF]);
        assert_eq!(input.buffer, None);
        assert_eq!(input.actions.len(), 2);
        assert_eq!(total_bytes(&input), MAX_TOTAL_BYTES);

        TEST_RUNTIME
            .block_on(fuzz_writer(test_operator(), input))
            .expect("fuzz writer must succeed");
    }

    /// The shape of the 9-byte OSS-Fuzz reproducer: `buffer == 1` plus a
    /// trailing short size section parses into a bounded input and still
    /// completes the write -> close -> readback path.
    #[test]
    fn oss_fuzz_reproducer_shape_is_bounded() {
        let input = parse(&[0x01, 0, 0, 0, 0x00, 0x05, 0, 0, 0]);
        assert_eq!(input.buffer, Some(1));
        assert!(total_bytes(&input) <= MAX_CHUNKS);

        TEST_RUNTIME
            .block_on(fuzz_writer(test_operator(), input))
            .expect("fuzz writer must succeed");
    }
}
