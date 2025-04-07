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

use bytes::Bytes;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use super::error::parse_error;
use crate::raw::*;
use crate::*;

/// The request to query all properties of a file or directory.
///
/// rfc4918 9.1: retrieve all properties define in specification
static PROPFIND_REQUEST: &str = r#"<?xml version="1.0" encoding="utf-8" ?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>"#;

/// The header to specify the depth of the query.
///
/// Valid values are `0`, `1`, `infinity`.
///
/// - `0`: only to the resource itself.
/// - `1`: to the resource and its internal members only.
/// - `infinity`: to the resource and all its members.
///
/// reference: [RFC4918: 10.2. Depth Header](https://datatracker.ietf.org/doc/html/rfc4918#section-10.2)
static HEADER_DEPTH: &str = "Depth";
/// The header to specify the destination of the query.
///
/// The Destination request header specifies the URI that identifies a
/// destination resource for methods such as COPY and MOVE, which take
/// two URIs as parameters.
///
/// reference: [RFC4918: 10.3.  Destination Header](https://datatracker.ietf.org/doc/html/rfc4918#section-10.3)
static HEADER_DESTINATION: &str = "Destination";
/// The header to specify the overwrite behavior of the query
///
/// The Overwrite request header specifies whether the server should
/// overwrite a resource mapped to the destination URL during a COPY or
/// MOVE.
///
/// Valid values are `T` and `F`.
///
/// A value of "F" states that the server must not perform the COPY or MOVE operation
/// if the destination URL does map to a resource.
///
/// reference: [RFC4918: 10.6.  Overwrite Header](https://datatracker.ietf.org/doc/html/rfc4918#section-10.6)
static HEADER_OVERWRITE: &str = "Overwrite";

pub struct WebdavCore {
    pub info: Arc<AccessorInfo>,
    pub endpoint: String,
    pub server_path: String,
    pub root: String,
    pub authorization: Option<String>,
}

impl Debug for WebdavCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebdavCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl WebdavCore {
    pub async fn webdav_stat(&self, path: &str) -> Result<Metadata> {
        let path = build_rooted_abs_path(&self.root, path);
        self.webdav_stat_rooted_abs_path(&path).await
    }

    /// Input path must be `rooted_abs_path`.
    async fn webdav_stat_rooted_abs_path(&self, rooted_abs_path: &str) -> Result<Metadata> {
        let url = format!("{}{}", self.endpoint, percent_encode_path(rooted_abs_path));
        let mut req = Request::builder().method("PROPFIND").uri(url);

        req = req.header(header::CONTENT_TYPE, "application/xml");
        req = req.header(header::CONTENT_LENGTH, PROPFIND_REQUEST.len());
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        // Only stat the resource itself.
        req = req.header(HEADER_DEPTH, "0");

        let req = req
            .body(Buffer::from(Bytes::from(PROPFIND_REQUEST)))
            .map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let result: Multistatus = deserialize_multistatus(&bs.to_bytes())?;
        let propfind_resp = result.response.first().ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "propfind response is empty, the resource is not exist",
            )
        })?;

        let metadata = parse_propstat(&propfind_resp.propstat)?;
        Ok(metadata)
    }

    pub async fn webdav_get(
        &self,
        path: &str,
        range: BytesRange,
        _: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url: String = format!("{}{}", self.endpoint, percent_encode_path(&path));

        let mut req = Request::get(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn webdav_put(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url = format!("{}{}", self.endpoint, percent_encode_path(&path));

        let mut req = Request::put(&url);

        if let Some(v) = &self.authorization {
            req = req.header(header::AUTHORIZATION, v)
        }

        if let Some(v) = size {
            req = req.header(header::CONTENT_LENGTH, v)
        }

        if let Some(v) = args.content_type() {
            req = req.header(header::CONTENT_TYPE, v)
        }

        if let Some(v) = args.content_disposition() {
            req = req.header(header::CONTENT_DISPOSITION, v)
        }

        let req = req.body(body).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn webdav_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url = format!("{}{}", self.endpoint, percent_encode_path(&path));

        let mut req = Request::delete(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn webdav_copy(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        // Check if source file exists.
        let _ = self.webdav_stat(from).await?;
        // Make sure target's dir is exist.
        self.webdav_mkcol(get_parent(to)).await?;

        let source = build_rooted_abs_path(&self.root, from);
        let source_uri = format!("{}{}", self.endpoint, percent_encode_path(&source));

        let target = build_rooted_abs_path(&self.root, to);
        let target_uri = format!("{}{}", self.endpoint, percent_encode_path(&target));

        let mut req = Request::builder().method("COPY").uri(&source_uri);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        req = req.header(HEADER_DESTINATION, target_uri);
        req = req.header(HEADER_OVERWRITE, "T");

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn webdav_move(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        // Check if source file exists.
        let _ = self.webdav_stat(from).await?;
        // Make sure target's dir is exist.
        self.webdav_mkcol(get_parent(to)).await?;

        let source = build_rooted_abs_path(&self.root, from);
        let source_uri = format!("{}{}", self.endpoint, percent_encode_path(&source));

        let target = build_rooted_abs_path(&self.root, to);
        let target_uri = format!("{}{}", self.endpoint, percent_encode_path(&target));

        let mut req = Request::builder().method("MOVE").uri(&source_uri);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        req = req.header(HEADER_DESTINATION, target_uri);
        req = req.header(HEADER_OVERWRITE, "T");

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    pub async fn webdav_list(&self, path: &str, args: &OpList) -> Result<Response<Buffer>> {
        let path = build_rooted_abs_path(&self.root, path);
        let url = format!("{}{}", self.endpoint, percent_encode_path(&path));

        let mut req = Request::builder().method("PROPFIND").uri(&url);

        req = req.header(header::CONTENT_TYPE, "application/xml");
        req = req.header(header::CONTENT_LENGTH, PROPFIND_REQUEST.len());
        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth);
        }

        if args.recursive() {
            req = req.header(HEADER_DEPTH, "infinity");
        } else {
            req = req.header(HEADER_DEPTH, "1");
        }

        let req = req
            .body(Buffer::from(Bytes::from(PROPFIND_REQUEST)))
            .map_err(new_request_build_error)?;

        self.info.http_client().send(req).await
    }

    /// Create dir recursively for given path.
    ///
    /// # Notes
    ///
    /// We only expose this method to the backend since there are dependencies on input path.
    pub async fn webdav_mkcol(&self, path: &str) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, path);
        let mut path = path.as_str();

        let mut dirs = VecDeque::default();

        loop {
            match self.webdav_stat_rooted_abs_path(path).await {
                // Dir exists, break the loop.
                Ok(_) => {
                    break;
                }
                // Dir not found, keep going.
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    dirs.push_front(path);
                    path = get_parent(path);
                }
                // Unexpected error found, return it.
                Err(err) => return Err(err),
            }

            if path == "/" {
                break;
            }
        }

        for dir in dirs {
            self.webdav_mkcol_rooted_abs_path(dir).await?;
        }
        Ok(())
    }

    /// Create a dir
    ///
    /// Input path must be `rooted_abs_path`
    ///
    /// Reference: [RFC4918: 9.3.1.  MKCOL Status Codes](https://datatracker.ietf.org/doc/html/rfc4918#section-9.3.1)
    async fn webdav_mkcol_rooted_abs_path(&self, rooted_abs_path: &str) -> Result<()> {
        let url = format!("{}{}", self.endpoint, percent_encode_path(rooted_abs_path));

        let mut req = Request::builder().method("MKCOL").uri(&url);

        if let Some(auth) = &self.authorization {
            req = req.header(header::AUTHORIZATION, auth.clone())
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.info.http_client().send(req).await?;
        let status = resp.status();

        match status {
            // 201 (Created) - The collection was created.
            StatusCode::CREATED
            // 405 (Method Not Allowed) - MKCOL can only be executed on an unmapped URL.
            //
            // The MKCOL method can only be performed on a deleted or non-existent resource.
            // This error means the directory already exists which is allowed by create_dir.
            | StatusCode::METHOD_NOT_ALLOWED => {

                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }
}

pub fn deserialize_multistatus(bs: &[u8]) -> Result<Multistatus> {
    let s = String::from_utf8_lossy(bs);
    // HACKS! HACKS! HACKS!
    //
    // Make sure the string is escaped.
    // Related to <https://github.com/tafia/quick-xml/issues/719>
    //
    // This is a temporary solution, we should find a better way to handle this.
    let s = s.replace("&()_+-=;", "%26%28%29_%2B-%3D%3B");

    quick_xml::de::from_str(&s).map_err(new_xml_deserialize_error)
}

pub fn parse_propstat(propstat: &Propstat) -> Result<Metadata> {
    let Propstat {
        prop:
            Prop {
                getlastmodified,
                getcontentlength,
                getcontenttype,
                getetag,
                resourcetype,
                ..
            },
        status,
    } = propstat;

    if let [_, code, text] = status.splitn(3, ' ').collect::<Vec<_>>()[..3] {
        // As defined in https://tools.ietf.org/html/rfc2068#section-6.1
        let code = code.parse::<u16>().unwrap();
        if code >= 400 {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("propfind response is unexpected: {} {}", code, text),
            ));
        }
    }

    let mode: EntryMode = if resourcetype.value == Some(ResourceType::Collection) {
        EntryMode::DIR
    } else {
        EntryMode::FILE
    };
    let mut m = Metadata::new(mode);

    if let Some(v) = getcontentlength {
        m.set_content_length(v.parse::<u64>().unwrap());
    }

    if let Some(v) = getcontenttype {
        m.set_content_type(v);
    }

    if let Some(v) = getetag {
        m.set_etag(v);
    }

    // https://www.rfc-editor.org/rfc/rfc4918#section-14.18
    m.set_last_modified(parse_datetime_from_rfc2822(getlastmodified)?);

    // the storage services have returned all the properties
    Ok(m)
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone, Default)]
#[serde(default)]
pub struct Multistatus {
    pub response: Vec<PropfindResponse>,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct PropfindResponse {
    pub href: String,
    pub propstat: Propstat,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Propstat {
    pub status: String,
    pub prop: Prop,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Prop {
    pub getlastmodified: String,
    pub getetag: Option<String>,
    pub getcontentlength: Option<String>,
    pub getcontenttype: Option<String>,
    pub resourcetype: ResourceTypeContainer,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ResourceTypeContainer {
    #[serde(rename = "$value")]
    pub value: Option<ResourceType>,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    Collection,
}

#[cfg(test)]
mod tests {
    use quick_xml::de::from_str;

    use super::*;

    #[test]
    fn test_propstat() {
        let xml = r#"<D:propstat>
            <D:prop>
                <D:displayname>/</D:displayname>
                <D:getlastmodified>Tue, 01 May 2022 06:39:47 GMT</D:getlastmodified>
                <D:resourcetype><D:collection/></D:resourcetype>
                <D:lockdiscovery/>
                <D:supportedlock>
                    <D:lockentry>
                        <D:lockscope><D:exclusive/></D:lockscope>
                        <D:locktype><D:write/></D:locktype>
                    </D:lockentry>
                </D:supportedlock>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>"#;

        let propstat = from_str::<Propstat>(xml).unwrap();
        assert_eq!(
            propstat.prop.getlastmodified,
            "Tue, 01 May 2022 06:39:47 GMT"
        );
        assert_eq!(
            propstat.prop.resourcetype.value.unwrap(),
            ResourceType::Collection
        );

        assert_eq!(propstat.status, "HTTP/1.1 200 OK");
    }

    #[test]
    fn test_response_simple() {
        let xml = r#"<D:response>
            <D:href>/</D:href>
            <D:propstat>
                <D:prop>
                    <D:displayname>/</D:displayname>
                    <D:getlastmodified>Tue, 01 May 2022 06:39:47 GMT</D:getlastmodified>
                    <D:resourcetype><D:collection/></D:resourcetype>
                    <D:lockdiscovery/>
                    <D:supportedlock>
                        <D:lockentry>
                            <D:lockscope><D:exclusive/></D:lockscope>
                            <D:locktype><D:write/></D:locktype>
                        </D:lockentry>
                    </D:supportedlock>
                </D:prop>
                <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
        </D:response>"#;

        let response = from_str::<PropfindResponse>(xml).unwrap();
        assert_eq!(response.href, "/");

        assert_eq!(
            response.propstat.prop.getlastmodified,
            "Tue, 01 May 2022 06:39:47 GMT"
        );
        assert_eq!(
            response.propstat.prop.resourcetype.value.unwrap(),
            ResourceType::Collection
        );
        assert_eq!(response.propstat.status, "HTTP/1.1 200 OK");
    }

    #[test]
    fn test_response_file() {
        let xml = r#"<D:response>
        <D:href>/test_file</D:href>
        <D:propstat>
          <D:prop>
            <D:displayname>test_file</D:displayname>
            <D:getcontentlength>1</D:getcontentlength>
            <D:getlastmodified>Tue, 07 May 2022 05:52:22 GMT</D:getlastmodified>
            <D:resourcetype></D:resourcetype>
            <D:lockdiscovery />
            <D:supportedlock>
              <D:lockentry>
                <D:lockscope>
                  <D:exclusive />
                </D:lockscope>
                <D:locktype>
                  <D:write />
                </D:locktype>
              </D:lockentry>
            </D:supportedlock>
          </D:prop>
          <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
      </D:response>"#;

        let response = from_str::<PropfindResponse>(xml).unwrap();
        assert_eq!(response.href, "/test_file");
        assert_eq!(
            response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 05:52:22 GMT"
        );
        assert_eq!(response.propstat.prop.getcontentlength.unwrap(), "1");
        assert_eq!(response.propstat.prop.resourcetype.value, None);
        assert_eq!(response.propstat.status, "HTTP/1.1 200 OK");
    }

    #[test]
    fn test_with_multiple_items_simple() {
        let xml = r#"<D:multistatus xmlns:D="DAV:">
        <D:response>
        <D:href>/</D:href>
        <D:propstat>
            <D:prop>
                <D:displayname>/</D:displayname>
                <D:getlastmodified>Tue, 01 May 2022 06:39:47 GMT</D:getlastmodified>
                <D:resourcetype><D:collection/></D:resourcetype>
                <D:lockdiscovery/>
                <D:supportedlock>
                    <D:lockentry>
                        <D:lockscope><D:exclusive/></D:lockscope>
                        <D:locktype><D:write/></D:locktype>
                    </D:lockentry>
                </D:supportedlock>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
    </D:response>
    <D:response>
            <D:href>/</D:href>
            <D:propstat>
                <D:prop>
                    <D:displayname>/</D:displayname>
                    <D:getlastmodified>Tue, 01 May 2022 06:39:47 GMT</D:getlastmodified>
                    <D:resourcetype><D:collection/></D:resourcetype>
                    <D:lockdiscovery/>
                    <D:supportedlock>
                        <D:lockentry>
                            <D:lockscope><D:exclusive/></D:lockscope>
                            <D:locktype><D:write/></D:locktype>
                        </D:lockentry>
                    </D:supportedlock>
                </D:prop>
                <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
        </D:response>
        </D:multistatus>"#;

        let multistatus = from_str::<Multistatus>(xml).unwrap();

        let response = multistatus.response;
        assert_eq!(response.len(), 2);
        assert_eq!(response[0].href, "/");
        assert_eq!(
            response[0].propstat.prop.getlastmodified,
            "Tue, 01 May 2022 06:39:47 GMT"
        );
    }

    #[test]
    fn test_with_multiple_items_mixed() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
        <D:multistatus xmlns:D="DAV:">
          <D:response>
            <D:href>/</D:href>
            <D:propstat>
              <D:prop>
                <D:displayname>/</D:displayname>
                <D:getlastmodified>Tue, 07 May 2022 06:39:47 GMT</D:getlastmodified>
                <D:resourcetype>
                  <D:collection />
                </D:resourcetype>
                <D:lockdiscovery />
                <D:supportedlock>
                  <D:lockentry>
                    <D:lockscope>
                      <D:exclusive />
                    </D:lockscope>
                    <D:locktype>
                      <D:write />
                    </D:locktype>
                  </D:lockentry>
                </D:supportedlock>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
          <D:response>
            <D:href>/testdir/</D:href>
            <D:propstat>
              <D:prop>
                <D:displayname>testdir</D:displayname>
                <D:getlastmodified>Tue, 07 May 2022 06:40:10 GMT</D:getlastmodified>
                <D:resourcetype>
                  <D:collection />
                </D:resourcetype>
                <D:lockdiscovery />
                <D:supportedlock>
                  <D:lockentry>
                    <D:lockscope>
                      <D:exclusive />
                    </D:lockscope>
                    <D:locktype>
                      <D:write />
                    </D:locktype>
                  </D:lockentry>
                </D:supportedlock>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
          <D:response>
            <D:href>/test_file</D:href>
            <D:propstat>
              <D:prop>
                <D:displayname>test_file</D:displayname>
                <D:getcontentlength>1</D:getcontentlength>
                <D:getlastmodified>Tue, 07 May 2022 05:52:22 GMT</D:getlastmodified>
                <D:resourcetype></D:resourcetype>
                <D:lockdiscovery />
                <D:supportedlock>
                  <D:lockentry>
                    <D:lockscope>
                      <D:exclusive />
                    </D:lockscope>
                    <D:locktype>
                      <D:write />
                    </D:locktype>
                  </D:lockentry>
                </D:supportedlock>
              </D:prop>
              <D:status>HTTP/1.1 200 OK</D:status>
            </D:propstat>
          </D:response>
        </D:multistatus>"#;

        let multistatus = from_str::<Multistatus>(xml).unwrap();

        let response = multistatus.response;
        assert_eq!(response.len(), 3);
        let first_response = &response[0];
        assert_eq!(first_response.href, "/");
        assert_eq!(
            first_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:39:47 GMT"
        );

        let second_response = &response[1];
        assert_eq!(second_response.href, "/testdir/");
        assert_eq!(
            second_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:40:10 GMT"
        );

        let third_response = &response[2];
        assert_eq!(third_response.href, "/test_file");
        assert_eq!(
            third_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 05:52:22 GMT"
        );
    }

    #[test]
    fn test_with_multiple_items_mixed_nginx() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
      <D:multistatus xmlns:D="DAV:">
        <D:response>
          <D:href>/</D:href>
          <D:propstat>
            <D:prop>
              <D:getlastmodified>Fri, 17 Feb 2023 03:37:22 GMT</D:getlastmodified>
              <D:resourcetype>
                <D:collection />
              </D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_75</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_36</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_38</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_59</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_9</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_93</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_43</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
        <D:response>
          <D:href>/test_file_95</D:href>
          <D:propstat>
            <D:prop>
              <D:getcontentlength>1</D:getcontentlength>
              <D:getlastmodified>Fri, 17 Feb 2023 03:36:54 GMT</D:getlastmodified>
              <D:resourcetype></D:resourcetype>
            </D:prop>
            <D:status>HTTP/1.1 200 OK</D:status>
          </D:propstat>
        </D:response>
      </D:multistatus>
      "#;

        let multistatus: Multistatus = from_str(xml).unwrap();

        let response = multistatus.response;
        assert_eq!(response.len(), 9);

        let first_response = &response[0];
        assert_eq!(first_response.href, "/");
        assert_eq!(
            first_response.propstat.prop.getlastmodified,
            "Fri, 17 Feb 2023 03:37:22 GMT"
        );
    }
}
