// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::io::SeekFrom;
use std::usize;

use bytes::Bytes;
use log::debug;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::*;
use rand::prelude::*;
use sha2::Digest;
use sha2::Sha256;

/// Init a service with given scheme.
///
/// - If `opendal_{schema}_test` is on, construct a new Operator with given root.
/// - Else, returns a `None` to represent no valid config for operator.
pub fn init_service<B: Builder>(random_root: bool) -> Option<Operator> {
    let _ = env_logger::builder().is_test(true).try_init();
    let _ = dotenvy::dotenv();

    let prefix = format!("opendal_{}_", B::SCHEME);

    let mut cfg = env::vars()
        .filter_map(|(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect::<HashMap<String, String>>();

    let turn_on_test = cfg.get("test").cloned().unwrap_or_default();

    if turn_on_test != "on" && turn_on_test != "true" {
        return None;
    }

    if random_root {
        let root = format!(
            "{}{}/",
            cfg.get("root").cloned().unwrap_or_else(|| "/".to_string()),
            uuid::Uuid::new_v4()
        );
        cfg.insert("root".to_string(), root);
    }

    let op = Operator::from_map::<B>(cfg).expect("must succeed");

    #[cfg(feature = "layers-chaos")]
    let op = {
        use opendal::layers::ChaosLayer;
        op.layer(ChaosLayer::new(0.1))
    };

    let op = op
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish();

    Some(op)
}

pub fn gen_bytes() -> (Vec<u8>, usize) {
    let mut rng = thread_rng();

    let size = rng.gen_range(1..4 * 1024 * 1024);
    let mut content = vec![0; size];
    rng.fill_bytes(&mut content);

    (content, size)
}

pub fn gen_fixed_bytes(size: usize) -> Vec<u8> {
    let mut rng = thread_rng();

    let mut content = vec![0; size];
    rng.fill_bytes(&mut content);

    content
}

pub fn gen_offset_length(size: usize) -> (u64, u64) {
    let mut rng = thread_rng();

    // Make sure at least one byte is read.
    let offset = rng.gen_range(0..size - 1);
    let length = rng.gen_range(1..(size - offset));

    (offset as u64, length as u64)
}

/// ObjectReaderFuzzer is the fuzzer for object readers.
///
/// We will generate random read/seek/next operations to operate on object
/// reader to check if the output is expected.
///
/// # TODO
///
/// This fuzzer only generate valid operations.
///
/// In the future, we need to generate invalid operations to check if we
/// handled correctly.
pub struct ObjectReaderFuzzer {
    name: String,
    bs: Vec<u8>,

    offset: usize,
    size: usize,
    cur: usize,
    rng: ThreadRng,
    actions: Vec<ObjectReaderAction>,
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectReaderAction {
    Read(usize),
    Seek(SeekFrom),
    Next,
}

impl ObjectReaderFuzzer {
    /// Create a new fuzzer.
    pub fn new(name: &str, bs: Vec<u8>, offset: usize, size: usize) -> Self {
        Self {
            name: name.to_string(),
            bs,

            offset,
            size,
            cur: 0,

            rng: thread_rng(),
            actions: vec![],
        }
    }

    /// Generate a new action.
    pub fn fuzz(&mut self) -> ObjectReaderAction {
        let action = match self.rng.gen_range(0..3) {
            // Generate a read action.
            0 => {
                if self.cur >= self.size {
                    ObjectReaderAction::Read(0)
                } else {
                    let size = self.rng.gen_range(0..self.size - self.cur);
                    ObjectReaderAction::Read(size)
                }
            }
            // Generate a seek action.
            1 => match self.rng.gen_range(0..3) {
                // Generate a SeekFrom::Start action.
                0 => {
                    let offset = self.rng.gen_range(0..self.size as u64);
                    ObjectReaderAction::Seek(SeekFrom::Start(offset))
                }
                // Generate a SeekFrom::End action.
                1 => {
                    let offset = self.rng.gen_range(-(self.size as i64)..0);
                    ObjectReaderAction::Seek(SeekFrom::End(offset))
                }
                // Generate a SeekFrom::Current action.
                2 => {
                    let offset = self
                        .rng
                        .gen_range(-(self.cur as i64)..(self.size - self.cur) as i64);
                    ObjectReaderAction::Seek(SeekFrom::Current(offset))
                }
                _ => unreachable!(),
            },
            // Generate a next action.
            2 => ObjectReaderAction::Next,
            _ => unreachable!(),
        };

        debug!("{} perform fuzz action: {:?}", self.name, action);
        self.actions.push(action);

        action
    }

    /// Check if read operation is expected.
    pub fn check_read(&mut self, output_n: usize, output_bs: &[u8]) {
        assert!(
            self.cur + output_n <= self.size,
            "check read failed: output bs is larger than remaining bs: actions: {:?}",
            self.actions
        );

        let current_size = self.offset + self.cur;
        let expected_bs = &self.bs[current_size..current_size + output_n];

        assert_eq!(
            format!("{:x}", Sha256::digest(output_bs)),
            format!("{:x}", Sha256::digest(expected_bs)),
            "check read failed: output bs is different with expected bs, actions: {:?}",
            self.actions,
        );

        // Update current pos.
        self.cur += output_n;
    }

    /// Check if seek operation is expected.
    pub fn check_seek(&mut self, input_pos: SeekFrom, output_pos: u64) {
        let expected_pos = match input_pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.size as i64 + offset,
            SeekFrom::Current(offset) => self.cur as i64 + offset,
        };

        assert_eq!(
            output_pos, expected_pos as u64,
            "check seek failed: output pos is different with expected pos, actions: {:?}",
            self.actions
        );

        // Update current pos.
        self.cur = expected_pos as usize;
    }

    /// Check if next operation is expected.
    pub fn check_next(&mut self, output_bs: Option<Bytes>) {
        if let Some(output_bs) = output_bs {
            assert!(
                self.cur + output_bs.len() <= self.size,
                "check next failed: output bs is larger than remaining bs, actions: {:?}",
                self.actions
            );

            let current_size = self.offset + self.cur;
            let expected_bs = &self.bs[current_size..current_size + output_bs.len()];

            assert_eq!(
                format!("{:x}", Sha256::digest(&output_bs)),
                format!("{:x}", Sha256::digest(expected_bs)),
                "check next failed: output bs is different with expected bs, actions: {:?}",
                self.actions
            );

            // Update current pos.
            self.cur += output_bs.len();
        } else {
            assert!(
                self.cur >= self.size,
                "check next failed: output bs is None, we still have bytes to read, actions: {:?}",
                self.actions
            )
        }
    }
}
