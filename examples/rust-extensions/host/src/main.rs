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

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use opendal_extension_demo_runtime::opendal::{
    Buffer, ErrorKind, OperationContext, Operator, OperatorRegistry, OperatorUri,
};
use opendal_extension_demo_runtime::{LayerRegistry, load_extension, tokio};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let library = PathBuf::from(args.next().ok_or("missing extension path")?).canonicalize()?;
    let root =
        PathBuf::from(args.next().ok_or("missing temporary storage directory")?).canonicalize()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(library, root))
}

async fn run(library: PathBuf, root: PathBuf) -> Result<(), Box<dyn Error>> {
    let operators = OperatorRegistry::get();
    let layers = LayerRegistry::get();
    let uri = OperatorUri::new(
        "demo-fs",
        [(
            "root".to_owned(),
            root.to_str().ok_or("non-UTF-8 root")?.to_owned(),
        )],
    )?;
    assert_eq!(
        operators.load(uri.clone()).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(layers.load("demo-timeout").is_none());

    // The runner supplies the extension built together with this executable.
    unsafe { load_extension(&library)? };
    println!("PASS: dynamic registration, shared registries, host Tokio runtime");

    let base = operators.load(uri.clone())?;
    assert_eq!(base.info().scheme(), "fs");
    assert!(
        base.context()
            .executor()
            .clone()
            .into_inner()
            .timeout()
            .is_none()
    );
    let op = base.clone().layer(layers.load("demo-timeout").unwrap());
    assert!(!Arc::ptr_eq(base.service(), op.service()));

    // The plugin's context hook must survive replay when the host replaces its
    // base context. Poll its boxed future and execute a host future through it.
    let op = op.with_context(OperationContext::default());
    let executor = op.context().executor().clone().into_inner();
    let timeout = executor
        .timeout()
        .expect("plugin context hook supplies timeout");
    tokio::pin!(timeout);
    assert!(
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(timeout.as_mut().poll(cx).is_pending())
        })
        .await
    );
    tokio::time::timeout(Duration::from_secs(5), timeout).await?;
    let (send, recv) = tokio::sync::oneshot::channel();
    executor.execute(Box::pin(async move {
        send.send(42).unwrap();
    }));
    assert_eq!(recv.await?, 42);
    println!("PASS: layer service hook, context replay, Pending/wake, executor callback");

    let payload: Buffer = [
        Buffer::from("native ").to_bytes(),
        Buffer::from("Rust objects").to_bytes(),
    ]
    .into_iter()
    .collect();
    assert_eq!(Buffer::count(&payload), 2);
    let mut writer = op.writer("data.txt").await?;
    writer.write(payload).await?;
    writer.close().await?;
    drop(writer);
    assert_eq!(
        std::fs::read(root.join("data.txt"))?,
        b"native Rust objects"
    );
    assert_eq!(op.stat("data.txt").await?.content_length(), 19);
    let entries = op.list("").await?;
    assert!(entries.iter().any(|entry| entry.path() == "data.txt"));

    let reader = op.reader("data.txt").await?;
    drop(op);
    drop(base);
    let buffer = reader.read(7..11).await?;
    drop(reader);
    assert_eq!(buffer.to_vec(), b"Rust");
    println!("PASS: segmented write, close, filesystem oracle, stat/list, escaped range reader");

    // A plugin layer can also wrap a service constructed by the host.
    let memory =
        Operator::new(opendal_extension_demo_runtime::opendal::services::Memory::default())?
            .layer(layers.load("demo-timeout").unwrap());
    memory.write("copied", buffer.clone()).await?;
    assert_eq!(memory.read("copied").await?.to_vec(), b"Rust");
    drop(memory);
    assert_eq!(buffer.to_vec(), b"Rust");

    // Plugin factories still work after all earlier operators have been dropped.
    let reopened = operators.load(uri)?;
    assert_eq!(
        reopened.read("data.txt").await?.to_vec(),
        b"native Rust objects"
    );
    reopened.delete("data.txt").await?;
    assert!(!root.join("data.txt").exists());
    assert_eq!(
        reopened.read("data.txt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    println!("PASS: host-service composition, escaped Buffer, factory reuse, delete/error");
    Ok(())
}
