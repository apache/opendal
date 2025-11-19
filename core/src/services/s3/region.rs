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

use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::Mutex;

use http::Request;
use http::StatusCode;
use log::debug;
use log::warn;
use reqwest::Url;

use crate::Buffer;
use crate::raw::HttpClient;

pub type RegionCacheKey = (String, String);
pub type RegionCacheStore = HashMap<RegionCacheKey, String>;

pub static REGION_CACHE: LazyLock<Mutex<RegionCacheStore>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Allow constructing correct region endpoint if user gives a global endpoint.
pub static ENDPOINT_TEMPLATES: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        // AWS S3 Service.
        m.insert(
            "https://s3.amazonaws.com",
            "https://s3.{region}.amazonaws.com",
        );
        m
    });

#[derive(Debug, Clone)]
pub struct RegionProbe {
    endpoint: String,
    bucket: String,
    head_bucket_url: String,
}

impl RegionProbe {
    pub fn new(endpoint: &str, bucket: &str) -> Self {
        let bucket = bucket.to_string();
        // Remove the possible trailing `/` in endpoint.
        let endpoint = endpoint.trim_end_matches('/');

        // Make sure the endpoint contains the scheme, default to https otherwise.
        let endpoint = if endpoint.starts_with("http") {
            endpoint.to_string()
        } else {
            format!("https://{endpoint}")
        };

        // Remove bucket name from endpoint so heuristic matching works for both virtual-host
        // and path-style URLs.
        let endpoint = endpoint.replace(&format!("//{bucket}."), "//");
        let head_bucket_url = format!("{endpoint}/{bucket}");

        Self {
            endpoint,
            bucket,
            head_bucket_url,
        }
    }

    pub fn cache_key(&self) -> RegionCacheKey {
        (self.endpoint.clone(), self.bucket.clone())
    }

    pub fn lookup_cache(&self) -> Option<String> {
        REGION_CACHE
            .lock()
            .expect("region cache poisoned")
            .get(&self.cache_key())
            .cloned()
    }

    pub fn write_cache(&self, region: &str) {
        REGION_CACHE
            .lock()
            .expect("region cache poisoned")
            .insert(self.cache_key(), region.to_string());
    }

    pub fn heuristic_region(&self) -> Option<String> {
        let endpoint = self.endpoint.as_str();

        // Try to detect region by endpoint patterns before issuing network requests.

        // If this bucket is R2, we can return `auto` directly.
        // Reference: <https://developers.cloudflare.com/r2/api/s3/api/>
        if endpoint.ends_with("r2.cloudflarestorage.com") {
            return Some("auto".to_string());
        }

        // If this bucket is AWS, try to match the regional endpoint suffix.
        if let Some(v) = endpoint
            .strip_prefix("https://s3.")
            .or_else(|| endpoint.strip_prefix("http://s3."))
        {
            if let Some(region) = v.strip_suffix(".amazonaws.com") {
                return Some(region.to_string());
            }
        }

        // If this bucket is OSS, handle both public and internal endpoints.
        //
        // - `oss-ap-southeast-1.aliyuncs.com` => `oss-ap-southeast-1`
        // - `oss-cn-hangzhou-internal.aliyuncs.com` => `oss-cn-hangzhou`
        if let Some(v) = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
        {
            if let Some(region) = v.strip_suffix(".aliyuncs.com") {
                return Some(region.to_string());
            }

            if let Some(region) = v.strip_suffix("-internal.aliyuncs.com") {
                return Some(region.to_string());
            }
        }

        None
    }
}

pub async fn detect_region(endpoint: &str, bucket: &str) -> Option<String> {
    let target = RegionProbe::new(endpoint, bucket);

    if let Some(region) = target.lookup_cache() {
        return Some(region);
    }

    debug!(
        "detect region with url: {}",
        target.head_bucket_url.as_str()
    );

    if let Some(region) = target.heuristic_region() {
        target.write_cache(&region);
        return Some(region);
    }

    // Try to detect region by HeadBucket.
    let req = Request::head(&target.head_bucket_url)
        .body(Buffer::new())
        .ok()?;

    let client = HttpClient::new().ok()?;
    let res = client
        .send(req)
        .await
        .map_err(|err| warn!("detect region failed for: {err:?}"))
        .ok()?;

    debug!(
        "auto detect region got response: status {:?}, header: {:?}",
        res.status(),
        res.headers()
    );

    if let Some(header) = res.headers().get("x-amz-bucket-region") {
        if let Ok(region) = header.to_str() {
            let region = region.to_string();
            target.write_cache(&region);
            return Some(region);
        }
    }

    if res.status() == StatusCode::FORBIDDEN || res.status() == StatusCode::OK {
        let region = "us-east-1".to_string();
        target.write_cache(&region);
        return Some(region);
    }

    None
}

pub fn build_endpoint(
    configured_endpoint: Option<&str>,
    bucket: &str,
    enable_virtual_host_style: bool,
    region: &str,
) -> String {
    let mut endpoint = match configured_endpoint {
        Some(endpoint) => {
            if endpoint.starts_with("http") {
                endpoint.to_string()
            } else {
                // Prefix https if endpoint doesn't start with scheme.
                format!("https://{endpoint}")
            }
        }
        None => "https://s3.amazonaws.com".to_string(),
    };

    endpoint = endpoint.replace(&format!("//{bucket}."), "//");

    if let Ok(url) = Url::from_str(&endpoint) {
        endpoint = url.to_string().trim_end_matches('/').to_string();
    }

    endpoint = if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
        template.replace("{region}", region)
    } else {
        endpoint.to_string()
    };

    if enable_virtual_host_style {
        endpoint = endpoint.replace("//", &format!("//{bucket}."))
    } else {
        write!(endpoint, "/{bucket}").expect("write into string must succeed");
    };

    endpoint
}
