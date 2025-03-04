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

use std::io::BufReader;
use std::io::Cursor;

use anyhow::Result;
use log::debug;
use log::warn;
use sha2::Digest;
use sha2::Sha256;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.stat && cap.write && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_write_file,
            test_blocking_write_with_dir_path,
            test_blocking_write_with_special_chars,
            test_blocking_write_returns_metadata
        ))
    }

    if cap.write && cap.write_can_append && cap.stat && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_write_with_append,
            test_blocking_write_with_append_returns_metadata,
            test_blocking_writer_with_append
        ))
    }
}

/// Write a single file and test with stat.
pub fn test_blocking_write_file(op: BlockingOperator) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content)?;

    let meta = op.stat(&path).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);
    Ok(())
}

/// Write file with dir path should return an error
pub fn test_blocking_write_with_dir_path(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes(op.info().full_capability());

    let result = op.write(&path, content);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub fn test_blocking_write_with_special_chars(op: BlockingOperator) -> Result<()> {
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content)?;

    let meta = op.stat(&path).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);
    Ok(())
}

pub fn test_blocking_write_returns_metadata(op: BlockingOperator) -> Result<()> {
    let cap = op.info().full_capability();

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    let meta = op.write(&path, content)?;

    let stat_meta = op.stat(&path).expect("stat must succeed");

    assert_eq!(meta.content_length(), stat_meta.content_length());
    if cap.write_has_last_modified {
        assert_eq!(meta.last_modified(), stat_meta.last_modified());
    }
    if cap.write_has_etag {
        assert_eq!(meta.etag(), stat_meta.etag());
    }
    if cap.write_has_version {
        assert_eq!(meta.version(), stat_meta.version());
    }
    if cap.write_has_content_md5 {
        assert_eq!(meta.content_md5(), stat_meta.content_md5());
    }

    Ok(())
}

/// Test append to a file must success.
pub fn test_blocking_write_with_append(op: BlockingOperator) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    let (content_one, size_one) = gen_bytes(op.info().full_capability());
    let (content_two, size_two) = gen_bytes(op.info().full_capability());

    op.write_with(&path, content_one.clone())
        .append(true)
        .call()
        .expect("append file first time must success");

    op.write_with(&path, content_two.clone())
        .append(true)
        .call()
        .expect("append to an existing file must success");

    let bs = op.read(&path).expect("read file must success").to_bytes();

    assert_eq!(bs.len(), size_one + size_two);
    assert_eq!(bs[..size_one], content_one);
    assert_eq!(bs[size_one..], content_two);
    Ok(())
}

pub fn test_blocking_write_with_append_returns_metadata(op: BlockingOperator) -> Result<()> {
    let cap = op.info().full_capability();

    let (path, content_one, _) = TEST_FIXTURE.new_file(op.clone());

    op.write_with(&path, content_one)
        .append(true)
        .call()
        .expect("append file first time must success");

    let (data, _) = gen_bytes(op.info().full_capability());
    let meta = op
        .write_with(&path, data)
        .append(true)
        .call()
        .expect("append to an existing file must success");

    let stat_meta = op.stat(&path).expect("stat must succeed");

    assert_eq!(meta.content_length(), stat_meta.content_length());
    if cap.write_has_last_modified {
        assert_eq!(meta.last_modified(), stat_meta.last_modified());
    }
    if cap.write_has_etag {
        assert_eq!(meta.etag(), stat_meta.etag());
    }
    if cap.write_has_version {
        assert_eq!(meta.version(), stat_meta.version());
    }
    if cap.write_has_content_md5 {
        assert_eq!(meta.content_md5(), stat_meta.content_md5());
    }

    Ok(())
}

/// Copy data from reader to writer
pub fn test_blocking_writer_with_append(op: BlockingOperator) -> Result<()> {
    let path = TEST_FIXTURE.new_file_path();
    let (content, size): (Vec<u8>, usize) =
        gen_bytes_with_range(10 * 1024 * 1024..20 * 1024 * 1024);

    let mut a = op.writer_with(&path).append(true).call()?.into_std_write();

    // Wrap a buf reader here to make sure content is read in 1MiB chunks.
    let mut cursor = BufReader::with_capacity(1024 * 1024, Cursor::new(content.clone()));
    std::io::copy(&mut cursor, &mut a)?;
    a.close()?;

    let meta = op.stat(&path).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    let bs = op.read(&path)?.to_bytes();
    assert_eq!(bs.len(), size, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[..size])),
        format!("{:x}", Sha256::digest(content)),
        "read content"
    );
    Ok(())
}
