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

use http::{HeaderValue, Request, Response};
use serde::{Deserialize, Serialize};

use super::*;
use crate::raw::Service;
use crate::services::Memory;
use crate::{Buffer, HttpBody, HttpTransport, HttpTransporter};

fn memory_registry() -> OperatorRegistry {
    let registry = OperatorRegistry {
        factories: Mutex::default(),
    };
    crate::services::register_memory_service(&registry);
    registry
}

#[test]
fn global_registry_exposes_memory_scheme() {
    let schemes = OperatorRegistry::get().schemes();
    assert!(schemes.contains("memory"));
}

#[test]
fn unsupported_components_list_registered_names() {
    let registry = memory_registry();
    registry.register_context("configured", |ctx, _| Ok(ctx));
    registry.register_layer("configured", |_| Ok(MarkerLayer::new("unused")));
    for (uri, kind, available) in [
        ("missing://bucket/path", "scheme", "memory"),
        (
            "memory:///?context.missing.key=dummy-value",
            "context",
            "configured",
        ),
        ("memory:///?layers=missing", "layer", "configured"),
    ] {
        let err = registry.load(uri).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        let message = err.to_string();
        assert!(
            message.contains(&format!("{kind} is not registered")),
            "{message}"
        );
        assert!(message.contains("missing"), "{message}");
        assert!(message.contains("available"), "{message}");
        assert!(message.contains(available), "{message}");
        assert!(!message.contains("dummy-value"), "{message}");
    }
}

#[test]
fn validates_reserved_namespaces_and_layer_list() -> Result<()> {
    let registry = memory_registry();
    for (key, value) in [
        ("context.", "dummy-value"),
        ("context.alpha", "dummy-value"),
        ("context.alpha.", "dummy-value"),
        ("context..field", "dummy-value"),
        ("context.alpha!.field", "dummy-value"),
        ("layer.", "dummy-value"),
        ("layer..field", "dummy-value"),
        ("layer.alpha.", "dummy-value"),
        ("layer.alpha!.field", "dummy-value"),
        ("layers", "alpha,,beta"),
        ("layers", "alpha,"),
        ("layers", "alpha, ALPHA"),
        ("layers", "alpha!"),
        ("layers", "alpha\u{a0}"),
    ] {
        let err = registry.load(("memory", [(key, value)])).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "{key}={value}");
        assert!(!format!("{err:?}").contains(value));
    }
    for empty in ["", " \t\n"] {
        // Unregistered layer options are valid when the layer is inactive.
        registry.load(("memory", [("layers", empty), ("layer.inactive.field", "")]))?;
    }
    Ok(())
}

#[test]
fn uri_and_map_constructors_use_registry_while_typed_iter_is_service_only() -> Result<()> {
    let key = "context.phase_two_unregistered.field";
    let uri_err = Operator::from_uri(("memory", [(key, "dummy")])).unwrap_err();
    let map_err = Operator::via_iter("memory", [(key.to_owned(), "dummy".to_owned())]).unwrap_err();
    assert_eq!(uri_err.kind(), ErrorKind::Unsupported);
    assert_eq!(map_err.kind(), ErrorKind::Unsupported);

    let op = Operator::from_iter::<Memory>([
        ("root".to_owned(), "typed".to_owned()),
        (key.to_owned(), "dummy".to_owned()),
        ("layers".to_owned(), ",".to_owned()),
    ])?;
    assert_eq!(op.info().root(), "/typed/");
    Ok(())
}

#[tokio::test]
async fn load_filters_service_uri_and_composes_contexts_and_layers_once() -> Result<()> {
    let registry = memory_registry();
    registry.register::<UriFixture>("fixture");
    registry.register_context("Z-SECOND", |ctx, options| {
        assert_eq!(
            options,
            HashMap::from([("marker".to_owned(), "second".to_owned())])
        );
        Ok(
            ctx.with_http_transport(HttpTransporter::new(MarkerTransport {
                inner: ctx.http_transport().clone(),
                marker: "second",
            })),
        )
    });
    registry.register_context("A_first", |ctx, options| {
        assert_eq!(
            options,
            HashMap::from([("path.marker".to_owned(), "first".to_owned())])
        );
        Ok(ctx.with_http_transport(HttpTransporter::new(EchoTransport("first"))))
    });

    let hooks = Arc::new(Mutex::new(Vec::new()));
    let inner_hooks = Arc::clone(&hooks);
    registry.register_layer("INNER", move |options| {
        assert_eq!(
            options,
            HashMap::from([("marker".to_owned(), "inner".to_owned())])
        );
        Ok(MarkerLayer {
            marker: "inner",
            hooks: Arc::clone(&inner_hooks),
        })
    });
    let outer_hooks = Arc::clone(&hooks);
    registry.register_layer("outer", move |options| {
        assert!(options.is_empty());
        Ok(MarkerLayer {
            marker: "outer",
            hooks: Arc::clone(&outer_hooks),
        })
    });

    let op = registry.load((
        concat!(
            "fixture://dummy-user:dummy%2Dpass@host:8123/uri-root?root=query-root",
            "&service.option=query&Service.Option=query-last&Context.Z-SECOND.Marker=second",
            "&Context.A_FIRST.Path.Marker=first&LAYERS=%20INNER%2C%09Outer&layer.inner.marker=query",
        ),
        [("Root", "extra-root"), ("Layer.Inner.Marker", "inner"), ("layer.inactive.field", "ignored")],
    ))?;
    assert_eq!(op.info().root(), "/uri-root/");
    assert_eq!(
        *hooks.lock().unwrap(),
        [
            ("service", "inner"),
            ("service", "outer"),
            ("context", "inner"),
            ("context", "outer")
        ]
    );
    assert_eq!(markers(op.base_context()).await?, "second,first");
    assert_eq!(markers(op.context()).await?, "outer,inner,second,first");
    op.write("record", "memory data").await?;
    assert_eq!(op.read("record").await?.to_vec(), b"memory data");
    Ok(())
}

#[tokio::test]
async fn factories_can_replace_themselves_and_existing_operators_keep_their_snapshot() -> Result<()>
{
    let registry = Arc::new(memory_registry());
    let weak = Arc::downgrade(&registry);
    registry.register_context("test", move |ctx, _| {
        let registry = weak.upgrade().unwrap();
        assert!(
            registry.factories.try_lock().is_ok(),
            "context factory holds registry lock"
        );
        registry.register_context("TEST", |ctx, _| {
            Ok(ctx.with_http_transport(HttpTransporter::new(EchoTransport("new-context"))))
        });
        Ok(ctx.with_http_transport(HttpTransporter::new(EchoTransport("old-context"))))
    });
    let weak = Arc::downgrade(&registry);
    registry.register_layer("test", move |_| {
        let registry = weak.upgrade().unwrap();
        assert!(
            registry.factories.try_lock().is_ok(),
            "layer factory holds registry lock"
        );
        registry.register_layer("TEST", |_| Ok(MarkerLayer::new("new-layer")));
        Ok(MarkerLayer::new("old-layer"))
    });

    let uri = "memory:///?context.test.option=dummy&layers=test";
    let old = registry.load(uri)?;
    let new = registry.load(uri)?;
    assert_eq!(markers(new.context()).await?, "new-layer,new-context");
    assert_eq!(markers(old.context()).await?, "old-layer,old-context");
    let replay = old.clone().with_context(old.base_context().clone());
    assert_eq!(markers(replay.context()).await?, "old-layer,old-context");
    let new_base = old
        .base_context()
        .with_http_transport(HttpTransporter::new(EchoTransport("explicit")));
    let replay = old
        .with_context(new_base)
        .layer(MarkerLayer::new("appended"));
    assert_eq!(
        markers(replay.context()).await?,
        "appended,old-layer,explicit"
    );
    Ok(())
}

#[derive(Default)]
struct UriFixture(Memory);

#[derive(Debug, Serialize, Deserialize)]
struct UriFixtureConfig(String);

impl Builder for UriFixture {
    type Config = UriFixtureConfig;

    fn build(self) -> Result<impl Service> {
        self.0.build()
    }
}

impl Configurator for UriFixtureConfig {
    type Builder = UriFixture;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        assert_eq!(uri.scheme(), "fixture");
        assert_eq!(uri.authority(), Some("host:8123"));
        assert_eq!(uri.name(), Some("host"));
        assert_eq!(uri.username(), Some("dummy-user"));
        assert_eq!(uri.password(), Some("dummy-pass"));
        assert_eq!(
            uri.options(),
            &HashMap::from([
                ("root".to_owned(), "extra-root".to_owned()),
                ("service.option".to_owned(), "query-last".to_owned()),
            ])
        );
        Ok(Self(uri.root().unwrap().to_owned()))
    }

    fn into_builder(self) -> Self::Builder {
        UriFixture(Memory::default().root(&self.0))
    }
}

#[derive(Debug)]
struct MarkerLayer {
    marker: &'static str,
    hooks: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
}

impl MarkerLayer {
    fn new(marker: &'static str) -> Self {
        Self {
            marker,
            hooks: Arc::default(),
        }
    }
}

impl Layer for MarkerLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        self.hooks.lock().unwrap().push(("service", self.marker));
        inner
    }

    fn apply_context(&self, _: Servicer, ctx: OperationContext) -> OperationContext {
        self.hooks.lock().unwrap().push(("context", self.marker));
        ctx.with_http_transport(HttpTransporter::new(MarkerTransport {
            inner: ctx.http_transport().clone(),
            marker: self.marker,
        }))
    }
}

struct MarkerTransport {
    inner: HttpTransporter,
    marker: &'static str,
}

impl HttpTransport for MarkerTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> Result<Response<HttpBody>> {
        request
            .headers_mut()
            .append("x-marker", HeaderValue::from_static(self.marker));
        self.inner.fetch(request).await
    }
}

struct EchoTransport(&'static str);

impl HttpTransport for EchoTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        let markers = request.headers().get_all("x-marker");
        let values: Vec<_> = markers
            .iter()
            .map(|value| value.to_str().unwrap())
            .chain(std::iter::once(self.0))
            .collect();
        let body = Buffer::from(values.join(",").into_bytes());
        let size = body.len() as u64;
        Ok(Response::new(HttpBody::new(
            futures::stream::iter([Ok(body)]),
            Some(size),
        )))
    }
}

async fn markers(ctx: &OperationContext) -> Result<String> {
    let response = ctx
        .http_transport()
        .send(Request::new(Buffer::new()))
        .await?;
    Ok(String::from_utf8(response.into_body().to_vec()).unwrap())
}
