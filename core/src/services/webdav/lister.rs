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

use async_trait::async_trait;
use serde::Deserialize;
use std::str::FromStr;

use crate::raw::*;
use crate::*;

pub struct WebdavLister {
    server_path: String,
    root: String,
    path: String,
    multistates: Multistatus,
}

impl WebdavLister {
    /// TODO: sending request in `next_page` instead of in `new`.
    pub fn new(endpoint: &str, root: &str, path: &str, multistates: Multistatus) -> Self {
        // Some services might return the path with suffix `/remote.php/webdav/`, we need to trim them.
        let server_path = http::Uri::from_str(endpoint)
            .expect("must be valid http uri")
            .path()
            .trim_end_matches('/')
            .to_string();
        Self {
            server_path,
            root: root.into(),
            path: path.into(),
            multistates,
        }
    }
}

#[async_trait]
impl oio::PageList for WebdavLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // Build request instead of clone here.
        let oes = self.multistates.response.clone();

        for res in oes {
            let path = res
                .href
                .strip_prefix(&self.server_path)
                .unwrap_or(&res.href);

            // Ignore the root path itself.
            if self.root == path || self.root.trim_end_matches('/') == path {
                continue;
            }

            let normalized_path = build_rel_path(&self.root, path);
            let decoded_path = percent_decode_path(normalized_path.as_str());

            if normalized_path == self.path || decoded_path == self.path {
                // WebDav server may return the current path as an entry.
                continue;
            }

            let meta = res.parse_into_metadata()?;
            ctx.entries.push_back(oio::Entry::new(&decoded_path, meta))
        }
        ctx.done = true;

        Ok(())
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Multistatus {
    pub response: Vec<ListOpResponse>,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct MultistatusOptional {
    pub response: Option<Vec<ListOpResponse>>,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ListOpResponse {
    pub href: String,
    pub propstat: Propstat,
}

impl ListOpResponse {
    pub fn parse_into_metadata(&self) -> Result<Metadata> {
        let ListOpResponse {
            propstat:
                Propstat {
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
                },
            ..
        } = self;
        if let [_, code, text] = status.split(' ').collect::<Vec<_>>()[..3] {
            // As defined in https://tools.ietf.org/html/rfc2068#section-6.1
            let code = code.parse::<u16>().unwrap();
            if code >= 400 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    &format!("Invalid response: {} {}", code, text),
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
        Ok(m)
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Propstat {
    pub prop: Prop,
    pub status: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Prop {
    #[serde(default)]
    pub displayname: String,
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

        let response = from_str::<ListOpResponse>(xml).unwrap();
        assert_eq!(response.href, "/");

        assert_eq!(response.propstat.prop.displayname, "/");

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

        let response = from_str::<ListOpResponse>(xml).unwrap();
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
        assert_eq!(multistatus.response.len(), 2);
        assert_eq!(multistatus.response[0].href, "/");
        assert_eq!(
            multistatus.response[0].propstat.prop.getlastmodified,
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

        assert_eq!(multistatus.response.len(), 3);
        let first_response = &multistatus.response[0];
        assert_eq!(first_response.href, "/");
        assert_eq!(
            first_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:39:47 GMT"
        );

        let second_response = &multistatus.response[1];
        assert_eq!(second_response.href, "/testdir/");
        assert_eq!(
            second_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:40:10 GMT"
        );

        let third_response = &multistatus.response[2];
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

        assert_eq!(multistatus.response.len(), 9);

        let first_response = &multistatus.response[0];
        assert_eq!(first_response.href, "/");
        assert_eq!(
            first_response.propstat.prop.getlastmodified,
            "Fri, 17 Feb 2023 03:37:22 GMT"
        );
    }
}
