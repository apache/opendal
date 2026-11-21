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

use std::error::Error as _;
use std::time::Duration;

use opendal_core::{Configurator, ErrorKind, Operator};
use opendal_service_sftp::{Sftp, SftpConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

#[tokio::test]
async fn stalled_ssh_connection_respects_subsecond_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ssh://{}", listener.local_addr().unwrap());
    let builder = Sftp::default()
        .endpoint(&endpoint)
        .acquire_timeout(Duration::ZERO)
        .connect_timeout(Duration::from_millis(100));
    let pairs = [
        ("endpoint", endpoint.as_str()),
        ("acquire_timeout", "0s"),
        ("connect_timeout", "100ms"),
    ];
    let config =
        SftpConfig::from_iter(pairs.map(|(key, value)| (key.into(), value.into()))).unwrap();

    for builder in [builder, config.into_builder()] {
        let op = Operator::new(builder).unwrap();
        let error = timeout(Duration::from_secs(3), op.stat("file"))
            .await
            .expect("SSH connection must time out")
            .unwrap_err();
        assert!(error.is_temporary(), "{error:?}");
        assert!(
            error
                .to_string()
                .contains("SSH connection and authentication")
        );
        assert!(error.source().unwrap().is::<Elapsed>(), "{error:?}");
    }
}

// Use the key-auth fixture in fixtures/sftp/docker-compose-sftp.yml.
fn fixture_builder(endpoint: &str) -> Sftp {
    Sftp::default()
        .endpoint(endpoint)
        .user("foo")
        .key(&std::env::var("OPENDAL_SFTP_KEY").unwrap())
        .known_hosts_strategy("accept")
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn sshd_authentication_and_permission_failures_are_permanent() {
    let endpoint = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let op = Operator::new(fixture_builder(&endpoint)).unwrap();
    assert!(op.stat("upload/").await.unwrap().is_dir());

    let op = Operator::new(fixture_builder(&endpoint).user("invalid-opendal-user")).unwrap();
    let error = op.stat("upload/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(!error.is_temporary(), "{error:?}");
    let source = error.source().unwrap().downcast_ref::<openssh::Error>();
    assert!(matches!(source, Some(openssh::Error::Connect(_))));

    let op = Operator::new(fixture_builder(&endpoint).root("/forbidden")).unwrap();
    let error = op.stat("file").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(!error.is_temporary(), "{error:?}");
    assert!(error.to_string().contains("SFTP root initialization"));
    assert!(error.source().unwrap().is::<openssh_sftp_client::Error>());
}

#[tokio::test]
#[ignore = "requires the real OpenSSH SFTP fixture"]
async fn sshd_replaces_stalled_pooled_connection() {
    let upstream = std::env::var("OPENDAL_SFTP_ENDPOINT").unwrap();
    let upstream = upstream.strip_prefix("ssh://").unwrap().to_owned();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ssh://{}", proxy.local_addr().unwrap());
    let (stall, stalled) = oneshot::channel();
    let (ready, confirmed) = oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (mut untrusted, _) = proxy.accept().await.unwrap();
        let mut server = TcpStream::connect(&upstream).await.unwrap();
        tokio::io::copy_bidirectional(&mut untrusted, &mut server)
            .await
            .unwrap();

        let (mut first, _) = proxy.accept().await.unwrap();
        let mut server = TcpStream::connect(&upstream).await.unwrap();
        tokio::select! {
            result = tokio::io::copy_bidirectional(&mut first, &mut server) => {
                panic!("first SSH connection closed early: {result:?}");
            }
            _ = stalled => {}
        }
        // Leave the first socket open without forwarding any traffic to or from it.
        ready.send(()).unwrap();
        let (mut second, _) = proxy.accept().await.unwrap();
        let mut replacement = TcpStream::connect(&upstream).await.unwrap();
        tokio::io::copy_bidirectional(&mut second, &mut replacement)
            .await
            .unwrap();
    });

    let untrusted =
        Operator::new(fixture_builder(&endpoint).known_hosts_strategy("strict")).unwrap();
    let error = untrusted.stat("upload/").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(!error.is_temporary(), "{error:?}");
    assert!(error.source().unwrap().is::<openssh::Error>());

    let op = Operator::new(
        fixture_builder(&endpoint)
            .acquire_timeout(Duration::from_millis(25))
            .connect_timeout(Duration::from_secs(1)),
    )
    .unwrap();
    assert!(op.stat("upload/").await.unwrap().is_dir());
    stall.send(()).unwrap();
    confirmed.await.unwrap();
    let metadata = timeout(Duration::from_secs(5), op.stat("upload/"))
        .await
        .expect("checkout must discard the stalled connection and reconnect")
        .unwrap();
    assert!(metadata.is_dir());
    drop(op);
    proxy.abort();
    let _ = proxy.await;
}
