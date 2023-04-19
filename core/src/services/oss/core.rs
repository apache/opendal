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
use std::time::Duration;

use bytes::Bytes;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::header::RANGE;
use http::Request;
use http::Response;
use reqsign::AliyunCredential;
use reqsign::AliyunLoader;
use reqsign::AliyunOssSigner;
use serde::Deserialize;
use serde::Serialize;

use crate::ops::OpWrite;
use crate::raw::*;
use crate::*;

mod constants {
    pub const RESPONSE_CONTENT_DISPOSITION: &str = "response-content-disposition";
}

pub struct OssCore {
    pub root: String,
    pub bucket: String,
    /// buffered host string
    ///
    /// format: <bucket-name>.<endpoint-domain-name>
    pub host: String,
    pub endpoint: String,
    pub presign_endpoint: String,

    pub client: HttpClient,
    pub loader: AliyunLoader,
    pub signer: AliyunOssSigner,
}

impl Debug for OssCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl OssCore {
    async fn load_credential(&self) -> Result<Option<AliyunCredential>> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            Ok(Some(cred))
        } else {
            Ok(None)
        }
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = if let Some(cred) = self.load_credential().await? {
            cred
        } else {
            return Ok(());
        };

        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    pub async fn sign_query<T>(&self, req: &mut Request<T>, duration: Duration) -> Result<()> {
        let cred = if let Some(cred) = self.load_credential().await? {
            cred
        } else {
            return Ok(());
        };

        self.signer
            .sign_query(req, duration, &cred)
            .map_err(new_request_sign_error)
    }

    #[inline]
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        self.client.send(req).await
    }
}

impl OssCore {
    #[allow(clippy::too_many_arguments)]
    pub fn oss_put_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        cache_control: Option<&str>,
        body: AsyncBody,
        is_presign: bool,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size.unwrap_or_default());

        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime);
        }

        if let Some(pos) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        if let Some(cache_control) = cache_control {
            req = req.header(CACHE_CONTROL, cache_control)
        }

        let req = req.body(body).map_err(new_request_build_error)?;
        Ok(req)
    }

    pub fn oss_get_object_request(
        &self,
        path: &str,
        range: BytesRange,
        is_presign: bool,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        override_content_disposition: Option<&str>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let mut url = format!("{}/{}", endpoint, percent_encode_path(&p));

        // Add query arguments to the URL based on response overrides
        let mut query_args = Vec::new();
        if let Some(override_content_disposition) = override_content_disposition {
            query_args.push(format!(
                "{}={}",
                constants::RESPONSE_CONTENT_DISPOSITION,
                percent_encode_path(override_content_disposition)
            ))
        }

        if !query_args.is_empty() {
            url.push_str(&format!("?{}", query_args.join("&")));
        }

        let mut req = Request::get(&url);
        req = req.header(CONTENT_TYPE, "application/octet-stream");

        if !range.is_full() {
            req = req.header(RANGE, range.to_header());
            // Adding `x-oss-range-behavior` header to use standard behavior.
            // ref: https://help.aliyun.com/document_detail/39571.html
            req = req.header("x-oss-range-behavior", "standard");
        }

        if let Some(if_match) = if_match {
            req = req.header(IF_MATCH, if_match)
        }
        if let Some(if_none_match) = if_none_match {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn oss_delete_object_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(false);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));
        let req = Request::delete(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_head_object_request(
        &self,
        path: &str,
        is_presign: bool,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}", endpoint, percent_encode_path(&p));

        let mut req = Request::head(&url);
        if let Some(if_match) = if_match {
            req = req.header(IF_MATCH, if_match)
        }
        if let Some(if_none_match) = if_none_match {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub fn oss_list_object_request(
        &self,
        path: &str,
        token: Option<&str>,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);

        let endpoint = self.get_endpoint(false);
        let url = format!(
            "{}/?list-type=2&delimiter={delimiter}&prefix={}{}{}",
            endpoint,
            percent_encode_path(&p),
            limit.map(|t| format!("&max-keys={t}")).unwrap_or_default(),
            token
                .map(|t| format!("&continuation-token={}", percent_encode_path(t)))
                .unwrap_or_default(),
        );

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        Ok(req)
    }

    pub async fn oss_get_object(
        &self,
        path: &str,
        range: BytesRange,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
        override_content_disposition: Option<&str>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_get_object_request(
            path,
            range,
            false,
            if_match,
            if_none_match,
            override_content_disposition,
        )?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_head_object(
        &self,
        path: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_head_object_request(path, false, if_match, if_none_match)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_put_object(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        cache_control: Option<&str>,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_put_object_request(
            path,
            size,
            content_type,
            content_disposition,
            cache_control,
            body,
            false,
        )?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_copy_object(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Response<IncomingAsyncBody>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}",
            self.get_endpoint(false),
            percent_encode_path(&target)
        );
        let source = format!("/{}/{}", self.bucket, percent_encode_path(&source));

        let mut req = Request::put(&url)
            .header("x-oss-copy-source", source)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_list_object(
        &self,
        path: &str,
        token: Option<&str>,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_list_object_request(path, token, delimiter, limit)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let mut req = self.oss_delete_object_request(path)?;
        self.sign(&mut req).await?;
        self.send(req).await
    }

    pub async fn oss_delete_objects(
        &self,
        paths: Vec<String>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("{}/?delete", self.endpoint);

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&DeleteObjectsRequest {
            object: paths
                .into_iter()
                .map(|path| DeleteObjectsRequestObject {
                    key: build_abs_path(&self.root, &path),
                })
                .collect(),
        })
        .map_err(new_xml_deserialize_error)?;

        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");
        // Set content-md5 as required by API.
        let req = req.header("CONTENT-MD5", format_content_md5(content.as_bytes()));

        let mut req = req
            .body(AsyncBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.send(req).await
    }

    fn get_endpoint(&self, is_presign: bool) -> &str {
        if is_presign {
            &self.presign_endpoint
        } else {
            &self.endpoint
        }
    }

    pub async fn oss_initiate_upload(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<IncomingAsyncBody>> {
        let cache_control = args.cache_control();
        let req = self
            .oss_initiate_upload_request(path, None, None, cache_control, AsyncBody::Empty, false)
            .await?;
        self.send(req).await
    }

    /// Creates a request that initiates multipart upload
    async fn oss_initiate_upload_request(
        &self,
        path: &str,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
        cache_control: Option<&str>,
        body: AsyncBody,
        is_presign: bool,
    ) -> Result<Request<AsyncBody>> {
        let path = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!("{}/{}?uploads", endpoint, percent_encode_path(&path));
        let mut req = Request::post(&url);
        if let Some(mime) = content_type {
            req = req.header(CONTENT_TYPE, mime);
        }
        if let Some(disposition) = content_disposition {
            req = req.header(CONTENT_DISPOSITION, disposition);
        }
        if let Some(cache_control) = cache_control {
            req = req.header(CACHE_CONTROL, cache_control);
        }

        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        Ok(req)
    }

    /// Creates a request to upload a part
    pub async fn oss_upload_part_request(
        &self,
        path: &str,
        upload_id: &str,
        part_number: usize,
        is_presign: bool,
        size: Option<u64>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);

        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            endpoint,
            percent_encode_path(&p),
            part_number,
            percent_encode_path(upload_id)
        );

        let mut req = Request::put(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size);
        }
        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        Ok(req)
    }

    pub async fn oss_complete_multipart_upload_request(
        &self,
        path: &str,
        upload_id: &str,
        is_presign: bool,
        parts: &[MultipartUploadPart],
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let endpoint = self.get_endpoint(is_presign);
        let url = format!(
            "{}/{}?uploadId={}",
            endpoint,
            percent_encode_path(&p),
            percent_encode_path(upload_id)
        );

        let req = Request::post(&url);

        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest {
            part: parts.to_vec(),
        })
        .map_err(new_xml_deserialize_error)?;
        // Make sure content length has been set to avoid post with chunked encoding.
        let req = req.header(CONTENT_LENGTH, content.len());
        // Set content-type to `application/xml` to avoid mixed with form post.
        let req = req.header(CONTENT_TYPE, "application/xml");

        let mut req = req
            .body(AsyncBody::Bytes(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;
        self.send(req).await
    }
}

/// Request of DeleteObjects.
#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "Delete", rename_all = "PascalCase")]
pub struct DeleteObjectsRequest {
    pub object: Vec<DeleteObjectsRequestObject>,
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsRequestObject {
    pub key: String,
}

/// Result of DeleteObjects.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename = "DeleteResult", rename_all = "PascalCase")]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeleteObjectsResultDeleted>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObjectsResultDeleted {
    pub key: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct DeleteObjectsResultError {
    pub code: String,
    pub key: String,
    pub message: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    #[cfg(test)]
    pub bucket: String,
    #[cfg(test)]
    pub key: String,
    pub upload_id: String,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct MultipartUploadPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub part: Vec<MultipartUploadPart>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    pub location: String,
    pub bucket: String,
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;

    use super::*;

    /// This example is from https://www.alibabacloud.com/help/zh/object-storage-service/latest/deletemultipleobjects
    #[test]
    fn test_serialize_delete_objects_request() {
        let req = DeleteObjectsRequest {
            object: vec![
                DeleteObjectsRequestObject {
                    key: "multipart.data".to_string(),
                },
                DeleteObjectsRequestObject {
                    key: "test.jpg".to_string(),
                },
                DeleteObjectsRequestObject {
                    key: "demo.jpg".to_string(),
                },
            ],
        };

        let actual = quick_xml::se::to_string(&req).expect("must succeed");

        pretty_assertions::assert_eq!(
            actual,
            r#"<Delete>
  <Object>
    <Key>multipart.data</Key>
  </Object>
  <Object>
    <Key>test.jpg</Key>
  </Object>
  <Object>
    <Key>demo.jpg</Key>
  </Object>
</Delete>"#
                // Cleanup space and new line
                .replace([' ', '\n'], "")
        )
    }

    /// This example is from https://www.alibabacloud.com/help/zh/object-storage-service/latest/deletemultipleobjects
    #[test]
    fn test_deserialize_delete_objects_result() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <Deleted>
       <Key>multipart.data</Key>
    </Deleted>
    <Deleted>
       <Key>test.jpg</Key>
    </Deleted>
    <Deleted>
       <Key>demo.jpg</Key>
    </Deleted>
</DeleteResult>"#,
        );

        let out: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.deleted.len(), 3);
        assert_eq!(out.deleted[0].key, "multipart.data");
        assert_eq!(out.deleted[1].key, "test.jpg");
        assert_eq!(out.deleted[2].key, "demo.jpg");
    }

    #[test]
    fn test_deserialize_initiate_multipart_upload_response() {
        let bs = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <Bucket>oss-example</Bucket>
    <Key>multipart.data</Key>
    <UploadId>0004B9894A22E5B1888A1E29F823****</UploadId>
</InitiateMultipartUploadResult>"#,
        );
        let out: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(bs.reader()).expect("must success");

        assert_eq!("0004B9894A22E5B1888A1E29F823****", out.upload_id);
        assert_eq!("multipart.data", out.key);
        assert_eq!("oss-example", out.bucket);
    }

    #[test]
    fn test_serialize_complete_multipart_upload_request() {
        let req = CompleteMultipartUploadRequest {
            part: vec![
                MultipartUploadPart {
                    part_number: 1,
                    etag: "\"3349DC700140D7F86A0784842780****\"".to_string(),
                },
                MultipartUploadPart {
                    part_number: 5,
                    etag: "\"8EFDA8BE206636A695359836FE0A****\"".to_string(),
                },
                MultipartUploadPart {
                    part_number: 8,
                    etag: "\"8C315065167132444177411FDA14****\"".to_string(),
                },
            ],
        };

        // quick_xml::se::to_string()
        let mut serializer = quick_xml::se::Serializer::new(String::new());
        serializer.indent(' ', 4);
        let serialized = req.serialize(serializer).unwrap();
        pretty_assertions::assert_eq!(
            serialized,
            r#"<CompleteMultipartUpload>
    <Part>
        <PartNumber>1</PartNumber>
        <ETag>"3349DC700140D7F86A0784842780****"</ETag>
    </Part>
    <Part>
        <PartNumber>5</PartNumber>
        <ETag>"8EFDA8BE206636A695359836FE0A****"</ETag>
    </Part>
    <Part>
        <PartNumber>8</PartNumber>
        <ETag>"8C315065167132444177411FDA14****"</ETag>
    </Part>
</CompleteMultipartUpload>"#
                .replace('"', "&quot;") /* Escape `"` by hand to address <https://github.com/tafia/quick-xml/issues/362> */
        )
    }

    #[test]
    fn test_deserialize_complete_oss_multipart_result() {
        let bytes = Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
    <EncodingType>url</EncodingType>
    <Location>http://oss-example.oss-cn-hangzhou.aliyuncs.com /multipart.data</Location>
    <Bucket>oss-example</Bucket>
    <Key>multipart.data</Key>
    <ETag>"B864DB6A936D376F9F8D3ED3BBE540****"</ETag>
</CompleteMultipartUploadResult>"#,
        );

        let result: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(bytes.reader()).unwrap();
        assert_eq!("\"B864DB6A936D376F9F8D3ED3BBE540****\"", result.etag);
        assert_eq!(
            "http://oss-example.oss-cn-hangzhou.aliyuncs.com /multipart.data",
            result.location
        );
        assert_eq!("oss-example", result.bucket);
        assert_eq!("multipart.data", result.key);
    }
}
