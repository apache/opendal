// Copyright 2022 Datafuse Labs.
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

use std::time::Duration;
use std::time::Instant;

use futures::io::copy;
use futures::io::Cursor;
use futures::StreamExt;

use crate::readers::*;

#[tokio::test]
async fn reader_stream() {
    let reader = Box::new(Cursor::new("Hello, world!"));
    let mut s = ReaderStream::new(reader);

    let mut bs = Vec::new();
    while let Some(chunk) = s.next().await {
        bs.extend_from_slice(&chunk.unwrap());
    }

    assert_eq!(&bs[..], "Hello, world!".to_string().as_bytes());
}

#[tokio::test]
async fn callback_reader() {
    let mut size = 0;

    let reader = CallbackReader::new(Box::new(Cursor::new("Hello, world!")), |n| size += n);

    let mut bs = Vec::new();
    let n = copy(reader, &mut bs).await.unwrap();

    assert_eq!(size, 13);
    assert_eq!(n, 13);
}

#[tokio::test]
async fn observe_reader() {
    let mut last_pending = None;
    let mut read_cost = Duration::default();
    let mut size = 0;

    let reader = ObserveReader::new(Box::new(Cursor::new("Hello, world!")), |e| {
        let start = match last_pending {
            None => Instant::now(),
            Some(t) => t,
        };
        match e {
            ReadEvent::Pending => last_pending = Some(start),
            ReadEvent::Read(n) => {
                last_pending = None;
                size += n;
            }
            _ => {}
        }
        read_cost += start.elapsed().to_owned();
    });

    let mut bs = Vec::new();
    let n = copy(reader, &mut bs).await.unwrap();

    println!("read time: {:?}", read_cost);
    assert_eq!(size, 13);
    assert_eq!(n, 13);
    assert!(!read_cost.is_zero());
}
