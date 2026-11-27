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

import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Link from "@docusaurus/Link";

export default function DownloadLink() {
  const { siteConfig } = useDocusaurusContext();
  const version = siteConfig.customFields.latestRelease;
  if (!version) {
    return (
      <Link to="https://downloads.apache.org/opendal/">available from Apache Downloads</Link>
    );
  }

  const artifact = `opendal/apache-opendal-core-${version}-src.tar.gz`;
  const source = `https://www.apache.org/dyn/closer.lua/${artifact}?action=download`;
  const download = `https://downloads.apache.org/${artifact}`;
  return (
    <>
      <Link to={source}>{version}</Link>{" "}
      (<Link to={`${download}.asc`}>asc</Link>, <Link to={`${download}.sha512`}>sha512</Link>)
    </>
  );
}
