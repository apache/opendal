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

use futures::TryFutureExt;
use http::Request;
use hyper::client::ResponseFuture;
use std::io::{Error, ErrorKind};
use std::ops::Deref;
use std::sync::Arc;
use std::thread;

use crate::http_util::wait::forward;
use crate::http_util::wait::timeout;
use log::{debug, error};
use time::Duration;
use tokio::runtime;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient {
    #[cfg(feature = "rustls")]
    inner: hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>,
    #[cfg(not(feature = "rustls"))]
    inner: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>,

    tx: UnboundedSender<(
        hyper::Request<hyper::Body>,
        oneshot::Sender<hyper::Result<hyper::Response<hyper::Body>>>,
    )>,
    handle: Arc<thread::JoinHandle<()>>,
}

#[cfg(not(feature = "rustls"))]
#[inline]
fn https_connector() -> hyper_tls::HttpsConnector<hyper::client::HttpConnector> {
    hyper_tls::HttpsConnector::new()
}

#[cfg(feature = "rustls")]
#[inline]
fn https_connector() -> hyper_rustls::HttpsConnector<hyper::client::HttpConnector> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        let hc = hyper::Client::builder()
            // Disable connection pool to address weird async runtime hang.
            //
            // ref: https://github.com/datafuselabs/opendal/issues/473
            .pool_max_idle_per_host(0)
            .build(https_connector());
        let shc = hc.clone();

        // Borrowed a lot from reqwest::blocking.
        let (tx, rx) = mpsc::unbounded_channel::<(
            hyper::Request<hyper::Body>,
            oneshot::Sender<hyper::Result<hyper::Response<hyper::Body>>>,
        )>();
        let (spawn_tx, spawn_rx) = oneshot::channel::<std::io::Result<()>>();
        let handle = thread::Builder::new()
            .name("opendal-http-sync-runtime".into())
            .spawn(move || {
                let rt = match runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(v) => v,
                    Err(e) => {
                        if let Err(_) = spawn_tx.send(Err(e)) {
                            panic!("failed to communicate successful startup");
                        }
                        return;
                    }
                };

                let f = async move {
                    if let Err(_) = spawn_tx.send(Ok(())) {
                        debug!("failed to communicate successful startup");
                        panic!("failed to communicate successful startup");
                    }

                    let mut rx = rx;
                    let shc = shc;

                    while let Some((req, req_tx)) = rx.recv().await {
                        let req_fut = shc.request(req);
                        tokio::spawn(forward(req_fut, req_tx));
                    }

                    debug!("({:?}) Receiver is shutdown", thread::current().id());
                };

                debug!("({:?}) start runtime::block_on", thread::current().id());
                rt.block_on(f);
                debug!("({:?}) end runtime::block_on", thread::current().id());
                drop(rt);
                debug!("({:?}) finished", thread::current().id());
            })
            .expect("start internal sync runtime failed");

        // Wait for the runtime thread to start up...
        match timeout(
            spawn_rx.map_err(|err| Error::new(ErrorKind::NotConnected, "cancelled")),
            None,
        ) {
            Ok(Ok(())) => (),
            Ok(Err(err)) => panic!("start internal sync runtime failed: {err:?}"),
            Err(_canceled) => event_loop_panicked(),
        }

        HttpClient {
            inner: hc,
            tx,
            handle: Arc::new(handle),
        }
    }

    pub fn request(&self, req: Request<hyper::Body>) -> ResponseFuture {
        self.inner.request(req)
    }

    pub fn blocking_request(
        &self,
        req: Request<hyper::Body>,
    ) -> hyper::Result<hyper::Response<hyper::Body>> {
        let (tx, rx) = oneshot::channel();

        self.tx.send((req, tx)).expect("core thread panicked");

        let f = async move { rx.await.map_err(|_canceled| event_loop_panicked()) };
        let result = timeout(f, Some(std::time::Duration::from_secs(10)));

        match result {
            Ok(Err(err)) => Err(err),
            Ok(Ok(res)) => Ok(res),
            Err(err) => unreachable!("unexpected end of request: {err:?}"),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}

#[cold]
#[inline(never)]
fn event_loop_panicked() -> ! {
    // The only possible reason there would be a Canceled error
    // is if the thread running the event loop panicked. We could return
    // an Err here, like a BrokenPipe, but the Client is not
    // recoverable. Additionally, the panic in the other thread
    // is not normal, and should likely be propagated.
    panic!("event loop thread panicked");
}
