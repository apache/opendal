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

use http::StatusCode;

use super::core::*;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

pub struct WebdavLister {
    core: Arc<WebdavCore>,
    ctx: OperationContext,

    path: String,
    args: OpList,
}

impl WebdavLister {
    pub fn new(core: Arc<WebdavCore>, ctx: OperationContext, path: &str, args: OpList) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
            args,
        }
    }

    fn resolve_href(&self, href: &str, is_dir: bool) -> Result<String> {
        let raw_path = extract_href_path(href);
        if !raw_path.starts_with('/') {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "webdav propfind href is not an absolute path or URI",
            )
            .with_context("href", href));
        }

        let stripped = if self.core.server_path.is_empty() {
            raw_path
        } else if raw_path == self.core.server_path {
            ""
        } else if let Some(rest) = raw_path.strip_prefix(&self.core.server_path)
            && rest.starts_with('/')
        {
            rest
        } else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "webdav propfind href does not start with server path",
            )
            .with_context("href", href)
            .with_context("server_path", &self.core.server_path));
        };

        let mut path = percent_decode_path(stripped);

        // Append `/` to path if it's a dir
        if !path.ends_with('/') && is_dir {
            path.push('/');
        }

        if path == self.core.root {
            return Ok("/".to_owned());
        }

        match path.strip_prefix(&self.core.root) {
            Some(rel) => Ok(rel.to_owned()),
            None => Err(Error::new(
                ErrorKind::Unexpected,
                "webdav propfind href is outside configured root",
            )
            .with_context("href", href)
            .with_context("root", &self.core.root)),
        }
    }
}

/// Extract the URI path component from a WebDAV `<D:href>` value.
///
/// Per RFC 4918 §8.3, `href` may be either a `path-absolute` (e.g. `/dav/file`)
/// or an `absolute-URI` (e.g. `https://host/dav/file`).
fn extract_href_path(href: &str) -> &str {
    if let Some((scheme, rest)) = href.split_once("://")
        && scheme
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
        && scheme
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    {
        let authority_and_path = rest.split(['?', '#']).next().unwrap_or("");
        return match authority_and_path.find('/') {
            Some(idx) => &authority_and_path[idx..],
            None => "/",
        };
    }

    href
}

impl oio::PageList for WebdavLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // PROPFIND enumerates a collection. Resolve a partial name in its parent.
        let directory = if self.path.ends_with('/') {
            self.path.as_str()
        } else {
            get_parent(&self.path)
        };
        let resp = self
            .core
            .webdav_list(&self.ctx, directory, &self.args)
            .await?;

        // jfrog artifactory's webdav services have some strange behavior.
        // We add this flag to check if the server is jfrog artifactory.
        //
        // Example: `"x-jfrog-version": "Artifactory/7.77.5 77705900"`
        let is_jfrog_artifactory = if let Some(v) = resp.headers().get("x-jfrog-version") {
            v.to_str().unwrap_or_default().starts_with("Artifactory")
        } else {
            false
        };

        let bs = if resp.status().is_success() {
            resp.into_body()
        } else if resp.status() == StatusCode::NOT_FOUND {
            ctx.done = true;
            return Ok(());
        } else {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("Propfind")),
                resp,
            ));
        };

        let result: Multistatus = deserialize_multistatus(&bs.to_bytes())?;

        for res in result.response {
            let meta = parse_propstats(&res.propstat)?;
            let normalized_path = self.resolve_href(&res.href, meta.is_dir())?;

            if directory != self.path && !normalized_path.starts_with(&self.path) {
                continue;
            }

            // HACKS! HACKS! HACKS!
            //
            // jfrog artifactory will generate a virtual checksum file for each file.
            // The checksum file can't be stated, but can be listed and read.
            // We ignore the checksum files to avoid listing unexpected files.
            if is_jfrog_artifactory && meta.content_type() == Some("application/x-checksum") {
                continue;
            }

            ctx.entries
                .push_back(oio::Entry::new(&normalized_path, meta))
        }
        ctx.done = true;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream;
    use http::{Request, Response, StatusCode};

    use super::super::backend::WebdavBuilder;
    use super::*;

    struct StaticPropfindTransport {
        body: String,
    }

    impl HttpTransport for StaticPropfindTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let data = Bytes::from(self.body.clone());
            let size = data.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::MULTI_STATUS)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(data))]),
                    Some(size),
                ))
                .unwrap())
        }
    }

    fn operator_with_propfind(endpoint: &str, root: &str, xml: &str) -> Result<Operator> {
        Ok(
            Operator::new(WebdavBuilder::default().endpoint(endpoint).root(root))?.with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    StaticPropfindTransport {
                        body: xml.to_string(),
                    },
                )),
            ),
        )
    }

    #[tokio::test]
    async fn test_list_handles_absolute_href_uris_and_extension_resourcetypes() -> Result<()> {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
        <D:multistatus xmlns:D="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
          <D:response>
            <D:href>http://localhost:8080/dav/sub</D:href>
            <D:propstat>
              <D:prop>
                <D:getlastmodified>Sat, 07 May 2022 06:39:47 GMT</D:getlastmodified>
                <D:resourcetype><D:collection/></D:resourcetype>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
          <D:response>
            <D:href>https://proxy.example.com/dav/sub/my%20file.txt?rev=1#frag</D:href>
            <D:propstat>
              <D:prop>
                <D:getcontentlength>12</D:getcontentlength>
                <D:getlastmodified>Sat, 07 May 2022 06:40:10 GMT</D:getlastmodified>
                <D:resourcetype><D:principal/></D:resourcetype>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
          <D:response>
            <D:href>http://localhost:8080/dav/sub/calendar</D:href>
            <D:propstat>
              <D:prop>
                <D:getlastmodified>Sat, 07 May 2022 06:40:10 GMT</D:getlastmodified>
                <D:resourcetype><D:collection/><cal:calendar/></D:resourcetype>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
        </D:multistatus>"#;

        let op = operator_with_propfind("http://localhost:8080/dav/", "/sub/", xml)?;
        let entries = op.list("").await?;

        let mut listed: Vec<(String, EntryMode)> = entries
            .into_iter()
            .map(|e| (e.path().to_string(), e.metadata().mode()))
            .collect();
        listed.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            listed,
            vec![
                ("/".to_string(), EntryMode::DIR),
                ("calendar/".to_string(), EntryMode::DIR),
                ("my file.txt".to_string(), EntryMode::FILE),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_list_handles_server_path_root_without_trailing_slash() -> Result<()> {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
        <D:multistatus xmlns:D="DAV:">
          <D:response>
            <D:href>/remote.php/webdav</D:href>
            <D:propstat>
              <D:prop>
                <D:getlastmodified>Sat, 07 May 2022 06:39:47 GMT</D:getlastmodified>
                <D:resourcetype><D:collection/></D:resourcetype>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
          <D:response>
            <D:href>/remote.php/webdav/a.txt</D:href>
            <D:propstat>
              <D:prop>
                <D:getcontentlength>3</D:getcontentlength>
                <D:getlastmodified>Sat, 07 May 2022 06:40:10 GMT</D:getlastmodified>
                <D:resourcetype/>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
        </D:multistatus>"#;

        let op = operator_with_propfind("http://localhost:8080/remote.php/webdav/", "/", xml)?;
        let entries = op.list("").await?;
        let mut paths: Vec<String> = entries.into_iter().map(|e| e.path().to_string()).collect();
        paths.sort();

        assert_eq!(paths, vec!["/".to_string(), "a.txt".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_rejects_short_empty_or_out_of_root_hrefs_without_panicking() -> Result<()> {
        let cases = [
            // Shorter than root
            ("http://localhost:8080", "/sub/", "<D:href>/</D:href>", true),
            // Empty href under non-root
            ("http://localhost:8080", "/sub/", "<D:href/>", false),
            // Empty href under root even when marked as collection
            ("http://localhost:8080", "/", "<D:href/>", true),
            // Sibling prefix sharing string prefix without path boundary
            (
                "http://localhost:8080",
                "/sub/",
                "<D:href>/sub2/file.txt</D:href>",
                false,
            ),
            // Absolute URI outside root
            (
                "http://localhost:8080",
                "/sub/",
                "<D:href>http://localhost:8080/other/file.txt</D:href>",
                false,
            ),
            // Outside server_path (shares prefix without slash boundary)
            (
                "http://localhost:8080/dav",
                "/",
                "<D:href>/dav2/file.txt</D:href>",
                false,
            ),
            // Non-absolute relative href
            (
                "http://localhost:8080",
                "/sub/",
                "<D:href>sub/file.txt</D:href>",
                false,
            ),
        ];

        for (endpoint, root, href_xml, is_dir) in cases {
            let prop_xml = if is_dir {
                "<D:resourcetype><D:collection/></D:resourcetype>"
            } else {
                "<D:getcontentlength>1</D:getcontentlength><D:resourcetype/>"
            };
            let xml = format!(
                r#"<?xml version="1.0" encoding="utf-8"?>
                <D:multistatus xmlns:D="DAV:">
                  <D:response>
                    {href_xml}
                    <D:propstat>
                      <D:prop>
                        <D:getlastmodified>Sat, 07 May 2022 06:39:47 GMT</D:getlastmodified>
                        {prop_xml}
                      </D:prop>
                      <D:status>HTTP/1.1 200 OK</D:status>
                    </D:propstat>
                  </D:response>
                </D:multistatus>"#
            );

            let op = operator_with_propfind(endpoint, root, &xml)?;
            let err = op.list("").await.expect_err(&format!(
                "href {href_xml} under {endpoint} / {root} must fail"
            ));
            assert_eq!(err.kind(), ErrorKind::Unexpected, "{href_xml}");
            assert!(
                err.to_string().contains("href"),
                "expected href error for {href_xml}, got: {err}"
            );
        }

        Ok(())
    }
}
