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
use std::fmt;

#[derive(Default, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct iCloudWebservicesResponse {
    #[serde(default)]
    pub hsa_challenge_required: bool,
    #[serde(default)]
    pub hsa_trusted_browser: bool,
    pub webservices: Webservices,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Webservices {
    pub drivews: Drivews,
    pub docws: Docws,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Drivews {
    // pcsRequired
    pub pcsRequired: bool,
    pub status: String,
    pub url: Option<String>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Docws {
    // pcsRequired
    pub pcsRequired: bool,
    pub status: String,
    pub url: Option<String>,
}

impl fmt::Display for iCloudWebservicesResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "iCloudWebservicesResponse, \
        hsa_challenge_required:{}, \
        hsa_trusted_browser:{}, \
        webservices:{:?}",
            &self.hsa_challenge_required, &self.hsa_trusted_browser, &self.webservices
        )
    }
}

impl fmt::Display for Webservices {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "iCloudWebservicesResponse, \
        drivews:{:?}",
            &self.drivews
        )
    }
}

impl fmt::Display for Drivews {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "iCloudWebservicesResponse, \
        url:{:?}",
            &self.url
        )
    }
}

impl fmt::Display for Docws {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "iCloudWebservicesResponse, \
        url:{:?}",
            &self.url
        )
    }
}
