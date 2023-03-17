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

use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq)]
pub struct Multistatus {
    pub response: Vec<ListOpResponse>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct ListOpResponse {
    pub href: String,
    pub propstat: Propstat,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Propstat {
    pub prop: Prop,
    pub status: String,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Prop {
    pub getlastmodified: String,
    pub resourcetype: ResourceTypeContainer,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct ResourceTypeContainer {
    #[serde(rename = "$value")]
    pub value: Option<ResourceType>,
}

#[derive(Deserialize, Debug, PartialEq)]
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
