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

#![cfg(all(
    feature = "services-http",
    any(
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls"
    ),
    not(target_arch = "wasm32")
))]

use std::time::Duration;

use opendal::ErrorKind;
use opendal::Operator;
use opendal::services::Http;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;

async fn serve_ranges(listener: TcpListener, responses: &[(&str, &str)]) -> anyhow::Result<()> {
    for &(range, response) in responses {
        let (mut stream, _) = listener.accept().await?;
        let mut request = BufReader::new(&mut stream).lines();
        assert_eq!(
            request.next_line().await?.as_deref(),
            Some("GET /file HTTP/1.1")
        );
        let mut range_header = None;
        while let Some(line) = request.next_line().await? {
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("range")
            {
                range_header = Some(value.trim().to_owned());
            }
        }
        assert_eq!(range_header.as_deref(), Some(range));
        let response = format!("HTTP/1.1 {response}");
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_limited_read_http_range_and_response_length() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let responses = [
        (
            "bytes=2-11",
            "206 Partial Content\r\nContent-Range: bytes 2-4/5\r\nContent-Length: 3\r\n\r\ncde",
        ),
        (
            "bytes=2-11",
            "206 Partial Content\r\nContent-Range: bytes 2-4/5\r\nContent-Length: 3\r\n\r\ncd",
        ),
        (
            "bytes=-4",
            "206 Partial Content\r\nContent-Range: bytes 1-4/5\r\nContent-Length: 4\r\n\r\nbcde",
        ),
        (
            "bytes=0-1",
            "412 Precondition Failed\r\nContent-Length: 0\r\n\r\n",
        ),
    ];

    opendal::install_default();
    let op = Operator::new(Http::default().endpoint(&endpoint))?;
    let client = async {
        let buf = op.read_with("file").range(2..).limit(10).await?;
        assert_eq!(buf.to_bytes(), "cde");

        let err = op.read_with("file").range(2..).limit(10).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let buf = op
            .read_with("file")
            .range(opendal::BytesRange::suffix(4))
            .limit(2)
            .await?;
        assert_eq!(buf.to_bytes(), "bc");

        let err = op
            .read_with("file")
            .if_match("\"stale\"")
            .limit(2)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        Ok(())
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(serve_ranges(listener, &responses), client)
    })
    .await??;
    Ok(())
}

#[cfg(feature = "layers-retry")]
#[tokio::test]
async fn test_truncated_http_range_resumption() -> anyhow::Result<()> {
    use futures::TryStreamExt;
    use opendal::layers::RetryLayer;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let truncated = (
        "bytes=2-9",
        "206 Partial Content\r\nContent-Range: bytes 2-9/12\r\nContent-Length: 8\r\n\r\nabc",
    );
    let responses = [
        truncated,
        truncated,
        (
            "bytes=5-9",
            "206 Partial Content\r\nContent-Range: bytes 5-9/12\r\nContent-Length: 5\r\n\r\ndefgh",
        ),
    ];

    opendal::install_default();
    let op = Operator::new(Http::default().endpoint(&endpoint))?;
    let client = async {
        let mut stream = op.reader("file").await?.into_stream(2..10).await?;
        let mut received = Vec::new();
        let error = loop {
            match stream.try_next().await {
                Ok(Some(chunk)) => received.extend_from_slice(&chunk.to_bytes()),
                Ok(None) => panic!("truncated response ended successfully"),
                Err(error) => break error,
            }
        };
        assert_eq!(received, b"abc");
        assert!(error.is_temporary(), "{error:?}");

        let op = op.layer(
            RetryLayer::new()
                .with_min_delay(Duration::ZERO)
                .with_max_times(1),
        );
        let data = op.read_with("file").range(2..10).await?;
        assert_eq!(data.to_bytes(), "abcdefgh");
        Ok(())
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(serve_ranges(listener, &responses), client)
    })
    .await??;
    Ok(())
}
