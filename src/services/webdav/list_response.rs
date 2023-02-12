use serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub struct LockEntry {
    pub lockscope: LockScopeContainer,
    pub locktype: LockTypeContainer,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct LockScopeContainer {
    #[serde(rename = "$value")]
    pub value: LockScope,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LockScope {
    Exclusive,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct LockTypeContainer {
    #[serde(rename = "$value")]
    pub value: LockType,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LockType {
    Write,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Multistatus {
    response: Vec<ListOpResponse>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct ListOpResponse {
    href: String,
    propstat: Propstat,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Propstat {
    prop: Prop,
    status: String,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Prop {
    displayname: String,
    getlastmodified: String,
    resourcetype: ResourceTypeContainer,
    lockdiscovery: (),
    supportedlock: SupportedLock,
}

#[derive(Deserialize, Debug, PartialEq)]
struct ResourceTypeContainer {
    #[serde(rename = "$value")]
    value: Option<ResourceType>,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ResourceType {
    Collection,
}

#[derive(Deserialize, Debug, PartialEq)]

struct SupportedLock {
    lockentry: LockEntry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::de::from_str;
    #[test]
    fn test_lockentry() {
        let xml = r#"<D:lockentry>
            <D:lockscope><D:exclusive/></D:lockscope>
            <D:locktype><D:write/></D:locktype>
        </D:lockentry>"#;

        let lockentry = from_str::<LockEntry>(xml).unwrap();
        assert_eq!(lockentry.lockscope.value, LockScope::Exclusive);
        assert_eq!(lockentry.locktype.value, LockType::Write);
    }

    #[test]
    fn test_supportedlock() {
        let xml = r#"<D:supportedlock>
            <D:lockentry>
                <D:lockscope><D:exclusive/></D:lockscope>
                <D:locktype><D:write/></D:locktype>
            </D:lockentry>
        </D:supportedlock>"#;

        let supportedlock = from_str::<SupportedLock>(xml).unwrap();
        assert_eq!(
            supportedlock.lockentry.lockscope.value,
            LockScope::Exclusive
        );
        assert_eq!(supportedlock.lockentry.locktype.value, LockType::Write);
    }

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
        assert_eq!(propstat.prop.displayname, "/");
        assert_eq!(
            propstat.prop.getlastmodified,
            "Tue, 01 May 2022 06:39:47 GMT"
        );
        assert_eq!(
            propstat.prop.resourcetype.value.unwrap(),
            ResourceType::Collection
        );
        assert_eq!(
            propstat.prop.supportedlock.lockentry.lockscope.value,
            LockScope::Exclusive
        );
        assert_eq!(
            propstat.prop.supportedlock.lockentry.locktype.value,
            LockType::Write
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
        assert_eq!(
            response
                .propstat
                .prop
                .supportedlock
                .lockentry
                .lockscope
                .value,
            LockScope::Exclusive
        );
        assert_eq!(
            response
                .propstat
                .prop
                .supportedlock
                .lockentry
                .locktype
                .value,
            LockType::Write
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
        assert_eq!(response.propstat.prop.displayname, "test_file");
        assert_eq!(
            response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 05:52:22 GMT"
        );
        assert_eq!(response.propstat.prop.resourcetype.value, None);

        assert_eq!(
            response
                .propstat
                .prop
                .supportedlock
                .lockentry
                .lockscope
                .value,
            LockScope::Exclusive
        );
        assert_eq!(
            response
                .propstat
                .prop
                .supportedlock
                .lockentry
                .locktype
                .value,
            LockType::Write
        );
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
        assert_eq!(multistatus.response[0].propstat.prop.displayname, "/");
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
        assert_eq!(first_response.propstat.prop.displayname, "/");
        assert_eq!(
            first_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:39:47 GMT"
        );

        let second_response = &multistatus.response[1];
        assert_eq!(second_response.href, "/testdir/");
        assert_eq!(second_response.propstat.prop.displayname, "testdir");
        assert_eq!(
            second_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 06:40:10 GMT"
        );

        let third_response = &multistatus.response[2];
        assert_eq!(third_response.href, "/test_file");
        assert_eq!(third_response.propstat.prop.displayname, "test_file");
        assert_eq!(
            third_response.propstat.prop.getlastmodified,
            "Tue, 07 May 2022 05:52:22 GMT"
        );
    }
}
