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

use std::sync::Arc;

use bytes::Buf;
use quick_xml::de;

use super::core::AzblobCore;
use super::core::ErrorContext;
use super::core::ListBlobsOutput;
use super::core::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct AzblobLister {
    core: Arc<AzblobCore>,
    ctx: OperationContext,

    path: String,
    delimiter: &'static str,
    limit: Option<usize>,
}

impl AzblobLister {
    pub fn new(
        core: Arc<AzblobCore>,
        ctx: OperationContext,
        path: String,
        recursive: bool,
        limit: Option<usize>,
    ) -> Self {
        let delimiter = if recursive { "" } else { "/" };

        Self {
            core,
            ctx,
            path,
            delimiter,
            limit,
        }
    }
}

impl oio::PageList for AzblobLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self
            .core
            .azblob_list_blobs(
                &self.ctx,
                &self.path,
                &ctx.token,
                self.delimiter,
                self.limit,
            )
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ListBlobs")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let output: ListBlobsOutput =
            de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // Try our best to check whether this list is done.
        //
        // - Check `next_marker`
        ctx.done = output.next_marker.as_deref().is_none_or(str::is_empty);
        ctx.token = output.next_marker.unwrap_or_default();

        let prefixes = output.blobs.blob_prefix;

        for prefix in prefixes {
            let de = oio::Entry::new(
                &build_relative_path(&self.core.root, &prefix.name),
                MetadataBuilder::dir().build(),
            );

            ctx.entries.push_back(de)
        }

        for object in output.blobs.blob {
            let mut path = build_relative_path(&self.core.root, &object.name);
            if path.is_empty() {
                path = "/".to_string();
            }

            let mut meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::file(object.properties.content_length)
            };
            let etag = object.properties.etag.as_str();
            if !etag.is_empty() {
                if etag.starts_with('"') && etag.ends_with('"') && etag.len() >= 2 {
                    meta.etag(etag);
                } else {
                    meta.etag(format!("\"{etag}\""));
                }
            }
            meta.content_md5(object.properties.content_md5)
                .content_type(object.properties.content_type)
                .last_modified(Timestamp::parse_rfc2822(
                    object.properties.last_modified.as_str(),
                )?);

            let de = oio::Entry::with(path, meta.build());
            ctx.entries.push_back(de);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use bytes::Bytes;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::*;

    use crate::backend::AzblobBuilder;

    #[derive(Clone)]
    struct SinglePageListTransport {
        body: &'static str,
        requests: Arc<AtomicUsize>,
    }

    impl HttpTransport for SinglePageListTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let n = self.requests.fetch_add(1, Ordering::Relaxed) + 1;
            assert!(
                n <= 2,
                "list must not loop infinitely when NextMarker is absent"
            );
            let bytes = Bytes::from_static(self.body.as_bytes());
            let size = bytes.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(
                    futures::stream::iter(vec![Ok(Buffer::from(bytes))]),
                    Some(size),
                ))
                .expect("mock response must build"))
        }
    }

    #[tokio::test]
    async fn list_stops_when_next_marker_missing() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://myaccount.blob.core.windows.net/" ContainerName="test">
  <Blobs>
    <Blob>
      <Name>append.log</Name>
      <Properties>
        <Last-Modified>Tue, 24 Nov 2026 01:24:30 GMT</Last-Modified>
        <Etag>0x8D123</Etag>
        <Content-Length>12</Content-Length>
        <Content-Type />
        <Content-MD5 />
      </Properties>
    </Blob>
    <Blob>
      <Name>quoted.bin</Name>
      <Properties>
        <Last-Modified>Tue, 24 Nov 2026 01:24:30 GMT</Last-Modified>
        <Etag>"0x8D456"</Etag>
        <Content-Length>34</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <Content-MD5>llJ/+jOlx5GdA1sL7SdKuw==</Content-MD5>
      </Properties>
    </Blob>
    <Blob>
      <Name>no-etag.bin</Name>
      <Properties>
        <Last-Modified>Tue, 24 Nov 2026 01:24:30 GMT</Last-Modified>
        <Etag />
        <Content-Length>0</Content-Length>
      </Properties>
    </Blob>
  </Blobs>
</EnumerationResults>"#;

        let requests = Arc::new(AtomicUsize::new(0));
        let transport = SinglePageListTransport {
            body: xml,
            requests: requests.clone(),
        };
        let op = Operator::new(
            AzblobBuilder::default()
                .container("test")
                .endpoint("https://myaccount.blob.core.windows.net")
                .skip_signature(),
        )
        .expect("azblob operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)));

        let entries = op.list("").await.expect("list must terminate cleanly");
        assert_eq!(requests.load(Ordering::Relaxed), 1);

        let by_path: std::collections::HashMap<_, _> = entries
            .into_iter()
            .map(|e| (e.path().to_string(), e.metadata().clone()))
            .collect();

        let append_meta = &by_path["append.log"];
        assert_eq!(append_meta.etag(), Some("\"0x8D123\""));
        assert_eq!(append_meta.content_md5(), Some(""));
        assert_eq!(append_meta.content_type(), Some(""));

        let quoted_meta = &by_path["quoted.bin"];
        assert_eq!(quoted_meta.etag(), Some("\"0x8D456\""));
        assert_eq!(quoted_meta.content_md5(), Some("llJ/+jOlx5GdA1sL7SdKuw=="));
        assert_eq!(quoted_meta.content_type(), Some("application/octet-stream"));

        let no_etag_meta = &by_path["no-etag.bin"];
        assert_eq!(no_etag_meta.etag(), None);
        assert_eq!(no_etag_meta.content_md5(), Some(""));
    }
}
