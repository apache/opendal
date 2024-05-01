import React from 'react';
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Link from "@docusaurus/Link";

function SourceTarballUrl() {
    const {
        siteConfig: {customFields},
    } = useDocusaurusContext();
    const version = customFields.version;
    // TODO: We can retrieve if staging as customFields.isStaging,
    //  but which links should we use for staging?
    const link = `https://www.apache.org/dyn/closer.lua/opendal/${version}/apache-opendal-core-${version}-src.tar.gz?action=download`;
    return <Link to={link}>0.45.1</Link>
}

function SignatureUrl() {
    const {
        siteConfig: {customFields},
    } = useDocusaurusContext();
    const version = customFields.version;
    const link = `https://downloads.apache.org/opendal/${version}/apache-opendal-core-${version}-src.tar.gz.asc`;
    return <Link to={link}>asc</Link>
}

function ChecksumUrl() {
    const {
        siteConfig: {customFields},
    } = useDocusaurusContext();
    const version = customFields.version;
    const link = `https://downloads.apache.org/opendal/${version}/apache-opendal-core-${version}-src.tar.gz.sha512`;
    return <Link to={link}>sha512</Link>
}

export default function DownloadLink() {
    return <>
        <SourceTarballUrl/> (<SignatureUrl/>, <ChecksumUrl/>)
    </>;
}