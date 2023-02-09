use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Multistatus {
    response: Vec<ListOpResponse>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct ListOpResponse {
    href: String,
    propstat: Propstat,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Propstat {
    prop: Prop,
    status: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Prop {
    displayname: String,
    getlastmodified: String,
    resourcetype: ResourceType,
    lockdiscovery: (),
    supportedlock: SupportedLock,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
#[derive(PartialEq)]
enum ResourceType {
    Collection,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct SupportedLock {
    lockentry: LockEntry,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct LockEntry {
    lockscope: LockScope,
    locktype: LockType,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
#[derive(PartialEq)]
enum LockScope {
    Exclusive,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
#[derive(PartialEq)]
enum LockType {
    Write,
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
        assert_eq!(lockentry.lockscope, LockScope::Exclusive);
        assert_eq!(lockentry.locktype, LockType::Write);
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
        assert_eq!(supportedlock.lockentry.lockscope, LockScope::Exclusive);
        assert_eq!(supportedlock.lockentry.locktype, LockType::Write);
    }

    #[test]
    fn test_propstat() {
        let xml = r#"<D:propstat>
            <D:prop>
                <D:displayname>/</D:displayname>
                <D:getlastmodified>Tue, 07 Feb 2023 06:39:47 GMT</D:getlastmodified>
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
            "Tue, 07 Feb 2023 06:39:47 GMT"
        );
        assert_eq!(propstat.prop.resourcetype, ResourceType::Collection);
        assert_eq!(
            propstat.prop.supportedlock.lockentry.lockscope,
            LockScope::Exclusive
        );
        assert_eq!(
            propstat.prop.supportedlock.lockentry.locktype,
            LockType::Write
        );
        assert_eq!(propstat.status, "HTTP/1.1 200 OK");
    }
}
