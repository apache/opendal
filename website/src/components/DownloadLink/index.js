/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Link from "@docusaurus/Link";

function SourceTarballUrl() {
    const {
        siteConfig: {customFields},
    } = useDocusaurusContext();
    const version = customFields.version;
    const link = `https://www.apache.org/dyn/closer.lua/opendal/${version}/apache-opendal-core-${version}-src.tar.gz?action=download`;
    return <Link to={link}>{version}</Link>
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