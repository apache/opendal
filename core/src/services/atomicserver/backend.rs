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

use std::fmt::Debug;
use std::fmt::Formatter;

use atomic_lib::agents::Agent;
use atomic_lib::client::get_authentication_headers;
use atomic_lib::commit::sign_message;
use bytes::Buf;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_TYPE;
use http::Request;
use serde::Deserialize;
use serde::Serialize;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::AtomicserverConfig;
use crate::*;

impl Configurator for AtomicserverConfig {
    type Builder = AtomicserverBuilder;
    fn into_builder(self) -> Self::Builder {
        AtomicserverBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AtomicserverBuilder {
    config: AtomicserverConfig,
}

impl Debug for AtomicserverBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicserverBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl AtomicserverBuilder {
    /// Set the root for Atomicserver.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }

    /// Set the server address for Atomicserver.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = Some(endpoint.into());
        self
    }

    /// Set the private key for agent used for Atomicserver.
    pub fn private_key(mut self, private_key: &str) -> Self {
        self.config.private_key = Some(private_key.into());
        self
    }

    /// Set the public key for agent used for Atomicserver.
    /// For example, if the subject URL for the agent being used
    /// is ${endpoint}/agents/lTB+W3C/2YfDu9IAVleEy34uCmb56iXXuzWCKBVwdRI=
    /// Then the required public key is `lTB+W3C/2YfDu9IAVleEy34uCmb56iXXuzWCKBVwdRI=`
    pub fn public_key(mut self, public_key: &str) -> Self {
        self.config.public_key = Some(public_key.into());
        self
    }

    /// Set the parent resource id (url) that Atomicserver uses to store resources under.
    pub fn parent_resource_id(mut self, parent_resource_id: &str) -> Self {
        self.config.parent_resource_id = Some(parent_resource_id.into());
        self
    }
}

impl Builder for AtomicserverBuilder {
    const SCHEME: Scheme = Scheme::Atomicserver;
    type Config = AtomicserverConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let endpoint = self.config.endpoint.clone().unwrap();
        let parent_resource_id = self.config.parent_resource_id.clone().unwrap();

        let agent = Agent {
            private_key: self.config.private_key.clone(),
            public_key: self.config.public_key.clone().unwrap(),
            subject: format!(
                "{}/agents/{}",
                endpoint,
                self.config.public_key.clone().unwrap()
            ),
            created_at: 1,
            name: Some("agent".to_string()),
        };

        Ok(AtomicserverBackend::new(Adapter {
            parent_resource_id,
            endpoint,
            agent,
            client: HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Atomicserver)
            })?,
        })
        .with_normalized_root(root))
    }
}

/// Backend for Atomicserver services.
pub type AtomicserverBackend = kv::Backend<Adapter>;

const FILENAME_PROPERTY: &str = "https://atomicdata.dev/properties/filename";

#[derive(Debug, Serialize)]
struct CommitStruct {
    #[serde(rename = "https://atomicdata.dev/properties/createdAt")]
    created_at: i64,
    #[serde(rename = "https://atomicdata.dev/properties/destroy")]
    destroy: bool,
    #[serde(rename = "https://atomicdata.dev/properties/isA")]
    is_a: Vec<String>,
    #[serde(rename = "https://atomicdata.dev/properties/signer")]
    signer: String,
    #[serde(rename = "https://atomicdata.dev/properties/subject")]
    subject: String,
}

#[derive(Debug, Serialize)]
struct CommitStructSigned {
    #[serde(rename = "https://atomicdata.dev/properties/createdAt")]
    created_at: i64,
    #[serde(rename = "https://atomicdata.dev/properties/destroy")]
    destroy: bool,
    #[serde(rename = "https://atomicdata.dev/properties/isA")]
    is_a: Vec<String>,
    #[serde(rename = "https://atomicdata.dev/properties/signature")]
    signature: String,
    #[serde(rename = "https://atomicdata.dev/properties/signer")]
    signer: String,
    #[serde(rename = "https://atomicdata.dev/properties/subject")]
    subject: String,
}

#[derive(Debug, Deserialize)]
struct FileStruct {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "https://atomicdata.dev/properties/downloadURL")]
    download_url: String,
}

#[derive(Debug, Deserialize)]
struct QueryResultStruct {
    #[serde(
        rename = "https://atomicdata.dev/properties/endpoint/results",
        default = "empty_vec"
    )]
    results: Vec<FileStruct>,
}

fn empty_vec() -> Vec<FileStruct> {
    Vec::new()
}

#[derive(Clone)]
pub struct Adapter {
    parent_resource_id: String,
    endpoint: String,
    agent: Agent,
    client: HttpClient,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.finish()
    }
}

impl Adapter {
    fn sign(&self, url: &str, mut req: http::request::Builder) -> http::request::Builder {
        let auth_headers = get_authentication_headers(url, &self.agent)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to get authentication headers",
                )
                .with_context("service", Scheme::Atomicserver)
                .set_source(err)
            })
            .unwrap();

        for (k, v) in &auth_headers {
            req = req.header(k, v);
        }

        req
    }
}

impl Adapter {
    pub fn atomic_get_object_request(&self, path: &str) -> Result<Request<Buffer>> {
        let path = normalize_path(path);
        let path = path.as_str();

        let filename_property_escaped = FILENAME_PROPERTY.replace(':', "\\:").replace('.', "\\.");
        let url = format!(
            "{}/search?filters={}:%22{}%22",
            self.endpoint,
            percent_encode_path(&filename_property_escaped),
            percent_encode_path(path)
        );

        let mut req = Request::get(&url);
        req = self.sign(&url, req);
        req = req.header(http::header::ACCEPT, "application/ad+json");

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        Ok(req)
    }

    fn atomic_post_object_request(&self, path: &str, value: Buffer) -> Result<Request<Buffer>> {
        let path = normalize_path(path);
        let path = path.as_str();

        let url = format!(
            "{}/upload?parent={}",
            self.endpoint,
            percent_encode_path(&self.parent_resource_id)
        );

        let mut req = Request::post(&url);
        req = self.sign(&url, req);

        let datapart = FormDataPart::new("assets")
            .header(
                CONTENT_DISPOSITION,
                format!("form-data; name=\"assets\"; filename=\"{}\"", path)
                    .parse()
                    .unwrap(),
            )
            .header(CONTENT_TYPE, "text/plain".parse().unwrap())
            .content(value.to_vec());

        let multipart = Multipart::new().part(datapart);
        let req = multipart.apply(req)?;

        Ok(req)
    }

    pub fn atomic_delete_object_request(&self, subject: &str) -> Result<Request<Buffer>> {
        let url = format!("{}/commit", self.endpoint);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("You're a time traveler")
            .as_millis() as i64;

        let commit_to_sign = CommitStruct {
            created_at: timestamp,
            destroy: true,
            is_a: ["https://atomicdata.dev/classes/Commit".to_string()].to_vec(),
            signer: self.agent.subject.to_string(),
            subject: subject.to_string().clone(),
        };
        let commit_sign_string =
            serde_json::to_string(&commit_to_sign).map_err(new_json_serialize_error)?;

        let signature = sign_message(
            &commit_sign_string,
            self.agent.private_key.as_ref().unwrap(),
            &self.agent.public_key,
        )
        .unwrap();

        let commit = CommitStructSigned {
            created_at: timestamp,
            destroy: true,
            is_a: ["https://atomicdata.dev/classes/Commit".to_string()].to_vec(),
            signature,
            signer: self.agent.subject.to_string(),
            subject: subject.to_string().clone(),
        };

        let req = Request::post(&url);
        let body_string = serde_json::to_string(&commit).map_err(new_json_serialize_error)?;

        let body_bytes = body_string.as_bytes().to_owned();
        let req = req
            .body(Buffer::from(body_bytes))
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn download_from_url(&self, download_url: &String) -> Result<Buffer> {
        let req = Request::get(download_url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        let resp = self.client.send(req).await?;
        Ok(resp.into_body())
    }
}

impl Adapter {
    async fn wait_for_resource(&self, path: &str, expect_exist: bool) -> Result<()> {
        // This is used to wait until insert/delete is actually effective
        // This wait function is needed because atomicserver commits are not processed in real-time
        // See https://docs.atomicdata.dev/commits/intro.html#motivation
        for _i in 0..1000 {
            let req = self.atomic_get_object_request(path)?;
            let resp = self.client.send(req).await?;
            let bytes = resp.into_body();
            let query_result: QueryResultStruct =
                serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;
            if !expect_exist && query_result.results.is_empty() {
                break;
            }
            if expect_exist && !query_result.results.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(30));
        }

        Ok(())
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Atomicserver,
            "atomicserver",
            Capability {
                read: true,
                write: true,
                delete: true,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let req = self.atomic_get_object_request(path)?;
        let resp = self.client.send(req).await?;
        let bytes = resp.into_body();

        let query_result: QueryResultStruct =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        if query_result.results.is_empty() {
            return Err(Error::new(
                ErrorKind::NotFound,
                "atomicserver: key not found",
            ));
        }

        let bytes_file = self
            .download_from_url(&query_result.results[0].download_url)
            .await?;

        Ok(Some(bytes_file))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let req = self.atomic_get_object_request(path)?;
        let res = self.client.send(req).await?;
        let bytes = res.into_body();

        let query_result: QueryResultStruct =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        for result in query_result.results {
            let req = self.atomic_delete_object_request(&result.id)?;
            let _res = self.client.send(req).await?;
        }

        let _ = self.wait_for_resource(path, false).await;

        let req = self.atomic_post_object_request(path, value)?;
        let _res = self.client.send(req).await?;
        let _ = self.wait_for_resource(path, true).await;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let req = self.atomic_get_object_request(path)?;
        let res = self.client.send(req).await?;
        let bytes = res.into_body();

        let query_result: QueryResultStruct =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        for result in query_result.results {
            let req = self.atomic_delete_object_request(&result.id)?;
            let _res = self.client.send(req).await?;
        }

        let _ = self.wait_for_resource(path, false).await;

        Ok(())
    }
}
