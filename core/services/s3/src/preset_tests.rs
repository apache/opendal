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

use std::time::Duration;

use http::{Method, Request, Response, StatusCode};
use opendal_core::Configurator;
use opendal_core::ErrorKind;
use opendal_core::Operator;
use opendal_core::OperatorRegistry;
use opendal_core::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Result};
use url::Url;

use super::*;

type Options<'a> = &'a [(&'a str, &'a str)];

fn registry() -> &'static OperatorRegistry {
    let registry = OperatorRegistry::get();
    for register in [
        register_akamai_service as fn(&OperatorRegistry),
        register_b2_s3_service,
        register_cos_s3_service,
        register_gcs_s3_service,
        register_lakefs_s3_service,
        register_minio_service,
        register_obs_s3_service,
        register_oss_s3_service,
        register_swift_s3api_service,
        register_tos_s3_service,
        register_ceph_rgw_service,
        register_exoscale_service,
        register_garage_service,
        register_hetzner_service,
        register_ibm_cos_service,
        register_idrive_e2_service,
        register_ionos_service,
        register_neon_service,
        register_oci_service,
        register_ovhcloud_service,
        register_qingstor_service,
        register_r2_service,
        register_rustfs_service,
        register_scaleway_service,
        register_seaweedfs_service,
        register_spaces_service,
        register_storj_service,
        register_supabase_service,
        register_tigris_service,
        register_wasabi_service,
    ] {
        register(registry);
    }
    registry
}

async fn presign(uri: &str, options: &[(&str, &str)]) -> (Url, String) {
    let credentials = [
        ("access_key_id", "test-access"),
        ("secret_access_key", "test-secret"),
    ];
    let op = registry()
        .load((uri, options.iter().copied().chain(credentials)))
        .unwrap_or_else(|error| panic!("{uri}: {error}"));
    let provider_uri = Url::parse(uri).unwrap();
    assert_eq!(op.info().scheme(), provider_uri.scheme());
    assert_eq!(op.info().root(), format!("{}/", provider_uri.path()));

    let request = op
        .presign_read("file", Duration::from_secs(60))
        .await
        .unwrap_or_else(|error| panic!("{uri}: {error}"));
    let url = Url::parse(&request.uri().to_string()).unwrap();
    let credential = url
        .query_pairs()
        .find_map(|(key, value)| (key == "X-Amz-Credential").then(|| value.into_owned()))
        .unwrap();
    let scope: Vec<_> = credential.split('/').collect();
    assert_eq!(scope[0], "test-access");
    assert_eq!(scope[3], "s3");
    (url, scope[2].to_owned())
}

#[tokio::test]
async fn provider_uris_use_documented_endpoints_and_signing_regions() {
    let cases: &[(&str, Options<'_>, &str, &str, &str)] = &[
        (
            "akamai://bucket/root",
            &[("endpoint", "https://us-sea-9.linodeobjects.com")],
            "bucket.us-sea-9.linodeobjects.com",
            "/root/file",
            "us-east-1",
        ),
        (
            "ceph-rgw://bucket/root",
            &[("endpoint", "https://rgw.example.com/stores/")],
            "rgw.example.com",
            "/stores/bucket/root/file",
            "us-east-1",
        ),
        (
            "garage://bucket/root",
            &[
                ("endpoint", "http://localhost:3900"),
                ("region", "my-garage"),
            ],
            "localhost",
            "/bucket/root/file",
            "my-garage",
        ),
        (
            "ibm-cos://bucket/root",
            &[(
                "endpoint",
                "https://s3.private.us.cloud-object-storage.appdomain.cloud",
            )],
            "s3.private.us.cloud-object-storage.appdomain.cloud",
            "/bucket/root/file",
            "us-standard",
        ),
        (
            "idrive-e2://bucket/root",
            &[("endpoint", "https://e2.example.com")],
            "e2.example.com",
            "/bucket/root/file",
            "us-east-1",
        ),
        (
            "ionos://bucket/root",
            &[("region", "de")],
            "s3.eu-central-1.ionoscloud.com",
            "/bucket/root/file",
            "de",
        ),
        (
            "neon://bucket/root",
            &[(
                "endpoint",
                "https://br-example.storage.c-6.us-east-2.aws.neon.tech/",
            )],
            "br-example.storage.c-6.us-east-2.aws.neon.tech",
            "/bucket/root/file",
            "us-east-2",
        ),
        (
            "rustfs://bucket/root",
            &[("endpoint", "http://localhost:9000")],
            "localhost",
            "/bucket/root/file",
            "us-east-1",
        ),
        (
            "seaweedfs://bucket/root",
            &[("endpoint", "http://localhost:8333")],
            "localhost",
            "/bucket/root/file",
            "us-east-1",
        ),
        (
            "storj://bucket/root",
            &[("endpoint", "https://gateway.storjshare.io")],
            "gateway.storjshare.io",
            "/bucket/root/file",
            "us-east-1",
        ),
        (
            "exoscale://bucket/root",
            &[("zone", "ch-dk-2")],
            "sos-ch-dk-2.exo.io",
            "/bucket/root/file",
            "ch-dk-2",
        ),
        (
            "hetzner://bucket/root",
            &[("location", "fsn1")],
            "bucket.fsn1.your-objectstorage.com",
            "/root/file",
            "fsn1",
        ),
        (
            "oci://namespace/root",
            &[("namespace", "namespace"), ("region", "us-ashburn-1")],
            "namespace.compat.objectstorage.us-ashburn-1.oci.customer-oci.com",
            "/namespace/root/file",
            "us-ashburn-1",
        ),
        (
            "ovhcloud://bucket/root",
            &[("region", "gra")],
            "bucket.s3.gra.io.cloud.ovh.net",
            "/root/file",
            "gra",
        ),
        (
            "qingstor://bucket/root",
            &[("zone", "pek3a")],
            "s3.pek3a.qsstor.com",
            "/bucket/root/file",
            "pek3a",
        ),
        (
            "scaleway://bucket/root",
            &[("region", "nl-ams")],
            "s3.nl-ams.scw.cloud",
            "/bucket/root/file",
            "nl-ams",
        ),
        (
            "spaces://lon1/root",
            &[("datacenter", "lon1")],
            concat!("lon1", ".lon1.digitaloceanspaces.com"),
            "/root/file",
            "us-east-1",
        ),
        (
            "supabase://bucket/root",
            &[("project_ref", "example-project"), ("region", "us-west-1")],
            "example-project.storage.supabase.co",
            "/storage/v1/s3/bucket/root/file",
            "us-west-1",
        ),
        (
            "tigris://bucket/root",
            &[],
            "bucket.t3.storage.dev",
            "/root/file",
            "auto",
        ),
        (
            "wasabi://bucket/root",
            &[("region", "eu-west-2")],
            "s3.eu-west-2.wasabisys.com",
            "/bucket/root/file",
            "eu-west-2",
        ),
        (
            "b2-s3://bucket/root",
            &[("region", "us-west-004")],
            "s3.us-west-004.backblazeb2.com",
            "/bucket/root/file",
            "us-west-004",
        ),
        (
            "gcs-s3://bucket.example.com/root",
            &[],
            "storage.googleapis.com",
            "/bucket.example.com/root/file",
            "auto",
        ),
        (
            "obs-s3://bucket/root?region=cn-north-1",
            &[],
            "bucket.obs.cn-north-1.myhuaweicloud.com",
            "/root/file",
            "cn-north-1",
        ),
        (
            "obs-s3://bucket/root",
            &[("region", "cn-north-4")],
            "bucket.obs.cn-north-4.myhuaweicloud.com",
            "/root/file",
            "cn-north-4",
        ),
        (
            "oss-s3://bucket/root",
            &[("region", "cn-hangzhou")],
            "bucket.s3.oss-cn-hangzhou.aliyuncs.com",
            "/root/file",
            "cn-hangzhou",
        ),
        (
            "cos-s3://bucket-name-1250000000/root",
            &[("region", "ap-guangzhou")],
            "bucket-name-1250000000.cos.ap-guangzhou.myqcloud.com",
            "/root/file",
            "ap-guangzhou",
        ),
        (
            "tos-s3://bucket/root",
            &[
                ("endpoint", "https://tos-s3-cn-beijing.volces.com"),
                ("region", "cn-beijing"),
            ],
            "bucket.tos-s3-cn-beijing.volces.com",
            "/root/file",
            "cn-beijing",
        ),
        (
            "tos-s3://bucket/root",
            &[
                ("endpoint", "https://tos-s3-cn-beijing.bytepluses.com.cn"),
                ("region", "cn-beijing"),
            ],
            "bucket.tos-s3-cn-beijing.bytepluses.com.cn",
            "/root/file",
            "cn-beijing",
        ),
        (
            "tos-s3://bucket/root",
            &[
                ("endpoint", "https://tos-s3-ap-southeast-1.bytepluses.com"),
                ("region", "ap-southeast-1"),
            ],
            "bucket.tos-s3-ap-southeast-1.bytepluses.com",
            "/root/file",
            "ap-southeast-1",
        ),
        (
            "swift-s3api://container/root",
            &[("endpoint", "https://swift.example.com")],
            "swift.example.com",
            "/container/root/file",
            "us-east-1",
        ),
        (
            "lakefs-s3://example-repo/dev/data",
            &[("endpoint", "https://lakefs.example.com")],
            "lakefs.example.com",
            "/example-repo/dev/data/file",
            "us-east-1",
        ),
    ];

    for &(uri, options, expected_host, expected_path, expected_region) in cases {
        let (url, region) = presign(uri, options).await;
        assert_eq!(url.host_str(), Some(expected_host), "{uri}");
        assert_eq!(url.path(), expected_path, "{uri}");
        assert_eq!(region, expected_region, "{uri}");
    }
}

#[tokio::test]
async fn explicit_endpoint_addressing_and_signing_options_take_precedence() {
    let (url, region) = presign(
        "spaces://bucket/root?datacenter=nyc3",
        &[
            ("endpoint", "http://127.0.0.1:9000/proxy/"),
            ("region", "custom-region"),
            ("enable_virtual_host_style", "false"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert_eq!(url.port(), Some(9000));
    assert_eq!(url.path(), "/proxy/bucket/root/file");
    assert_eq!(region, "custom-region");

    let (url, region) = presign(
        "wasabi://bucket/root",
        &[
            ("region", "custom-region"),
            ("endpoint", "https://s3.example.test"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("s3.example.test"));
    assert_eq!(region, "custom-region");

    let (url, region) = presign(
        "obs-s3://bucket/root?region=cn-north-4",
        &[("endpoint", "https://obs.example.test:8443")],
    )
    .await;
    assert_eq!(url.host_str(), Some("bucket.obs.example.test"));
    assert_eq!(url.port(), Some(8443));
    assert_eq!(url.path(), "/root/file");
    assert_eq!(region, "cn-north-4");
}

#[tokio::test]
async fn deployment_options_override_signing_region_and_addressing() {
    let (url, region) = presign(
        "ceph-rgw://bucket/root",
        &[
            ("endpoint", "https://rgw.example.com:8443"),
            ("region", "my-zonegroup"),
            ("enable_virtual_host_style", "true"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("bucket.rgw.example.com"));
    assert_eq!(url.port(), Some(8443));
    assert_eq!(url.path(), "/root/file");
    assert_eq!(region, "my-zonegroup");

    let (url, region) = presign(
        "ibm-cos://bucket/root",
        &[
            (
                "endpoint",
                "https://s3.eu-de.cloud-object-storage.appdomain.cloud",
            ),
            ("region", "eu-de"),
            ("enable_virtual_host_style", "true"),
        ],
    )
    .await;
    assert_eq!(
        url.host_str(),
        Some("bucket.s3.eu-de.cloud-object-storage.appdomain.cloud")
    );
    assert_eq!(url.path(), "/root/file");
    assert_eq!(region, "eu-de");

    let (url, region) = presign(
        "neon://bucket/root",
        &[
            ("endpoint", "https://proxy.example.com/neon"),
            ("region", "eu-west-1"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("proxy.example.com"));
    assert_eq!(url.path(), "/neon/bucket/root/file");
    assert_eq!(region, "eu-west-1");

    let (url, region) = presign(
        "ionos://bucket/root",
        &[
            ("endpoint", "https://s3.example.test"),
            ("region", "custom-region"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("s3.example.test"));
    assert_eq!(region, "custom-region");

    let (url, region) = presign("ionos://bucket/root", &[("region", "eu-central-4")]).await;
    assert_eq!(url.host_str(), Some("s3.eu-central-4.ionoscloud.com"));
    assert_eq!(url.path(), "/bucket/root/file");
    assert_eq!(region, "eu-central-4");

    let (url, region) = presign(
        "oss-s3://bucket/root",
        &[
            (
                "endpoint",
                "https://s3.oss-cn-hangzhou-internal.aliyuncs.com",
            ),
            ("region", "cn-hangzhou"),
        ],
    )
    .await;
    assert_eq!(
        url.host_str(),
        Some("bucket.s3.oss-cn-hangzhou-internal.aliyuncs.com")
    );
    assert_eq!(url.path(), "/root/file");
    assert_eq!(region, "cn-hangzhou");

    let (url, region) = presign(
        "swift-s3api://container/root",
        &[
            ("endpoint", "https://s3.swift.example.com"),
            ("region", "swift-local"),
            ("enable_virtual_host_style", "true"),
        ],
    )
    .await;
    assert_eq!(url.host_str(), Some("container.s3.swift.example.com"));
    assert_eq!(url.path(), "/root/file");
    assert_eq!(region, "swift-local");

    let (url, region) = presign(
        "lakefs-s3://team-a--example-repo/dev",
        &[
            ("endpoint", "https://s3.lakefs.example.com"),
            ("region", "lakefs-local"),
            ("enable_virtual_host_style", "true"),
        ],
    )
    .await;
    assert_eq!(
        url.host_str(),
        Some("team-a--example-repo.s3.lakefs.example.com")
    );
    assert_eq!(url.path(), "/dev/file");
    assert_eq!(region, "lakefs-local");
}

#[tokio::test]
async fn presets_presign_with_the_supplied_session_token() {
    for (uri, options, expected_region) in [
        (
            "supabase://bucket/root",
            &[
                ("endpoint", "http://127.0.0.1:54321/storage/v1/s3"),
                ("region", "local"),
            ][..],
            "local",
        ),
        ("tigris://bucket/root", &[][..], "auto"),
        (
            "r2://bucket/root",
            &[("account_id", "example-account")][..],
            "auto",
        ),
        (
            "minio://bucket/root",
            &[("endpoint", "https://minio.example.test")][..],
            "auto",
        ),
        (
            "wasabi://bucket/root",
            &[("region", "us-east-1")][..],
            "us-east-1",
        ),
        (
            "cos-s3://bucket-1250000000/root",
            &[("region", "ap-guangzhou")][..],
            "ap-guangzhou",
        ),
        (
            "obs-s3://bucket/root",
            &[("region", "cn-north-1")][..],
            "cn-north-1",
        ),
        (
            "oss-s3://bucket/root",
            &[("region", "cn-hangzhou")][..],
            "cn-hangzhou",
        ),
        (
            "lakefs-s3://example-repo/root",
            &[("endpoint", "https://lakefs.example.com")][..],
            "us-east-1",
        ),
    ] {
        let mut options = options.to_vec();
        options.extend([
            ("session_token", "test-token"),
            ("disable_config_load", "true"),
        ]);
        let (url, region) = presign(uri, &options).await;
        assert_eq!(region, expected_region, "{uri}");
        assert!(
            url.query_pairs().any(|(key, value)| {
                key.eq_ignore_ascii_case("x-amz-security-token") && value == "test-token"
            }),
            "{uri}"
        );
    }
}

#[tokio::test]
async fn obs_s3_put_uses_sigv4_with_unsigned_payload() -> Result<()> {
    struct CheckObsPut;

    impl HttpTransport for CheckObsPut {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(request.method(), Method::PUT);
            assert_eq!(
                request.uri(),
                "https://example-bucket.obs.cn-north-1.myhuaweicloud.com/prefix/file"
            );
            assert_eq!(request.body().to_vec(), b"test payload");
            let headers = request.headers();
            assert_eq!(headers["x-amz-content-sha256"], "UNSIGNED-PAYLOAD");
            assert_eq!(headers["x-amz-security-token"], "test-token");
            assert!(
                headers
                    .keys()
                    .all(|name| !name.as_str().starts_with("x-amz-checksum-"))
            );
            assert!(!headers.contains_key("x-amz-sdk-checksum-algorithm"));

            let auth = headers[http::header::AUTHORIZATION].to_str().unwrap();
            let (credential, signature_fields) = auth
                .strip_prefix("AWS4-HMAC-SHA256 Credential=")
                .unwrap()
                .split_once(", SignedHeaders=")
                .unwrap();
            let (access_key, scope) = credential.split_once('/').unwrap();
            assert_eq!(access_key, "test-access");
            let (_, region_service) = scope.split_once('/').unwrap();
            assert_eq!(region_service, "cn-north-1/s3/aws4_request");
            let (signed_headers, _) = signature_fields.split_once(", Signature=").unwrap();
            assert!(
                signed_headers
                    .split(';')
                    .any(|name| name == "x-amz-security-token")
            );

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    let op = registry()
        .load((
            "obs-s3://example-bucket/prefix?region=cn-north-1",
            [
                ("access_key_id", "test-access"),
                ("secret_access_key", "test-secret"),
                ("session_token", "test-token"),
            ],
        ))?
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(CheckObsPut)),
        );
    op.write("file", "test payload").await?;
    Ok(())
}

#[test]
fn provider_uris_require_explicit_credentials_when_config_loading_is_disabled() {
    for uri in [
        "tigris://bucket/root?disable_config_load=true",
        "r2://bucket/root?account_id=example-account&disable_config_load=true",
        "minio://bucket/root?endpoint=https%3A%2F%2Fminio.example.test&disable_config_load=true",
    ] {
        let error = registry().load(uri).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{uri}");
        assert!(
            error.to_string().contains(
                "access_key_id and secret_access_key are required when disable_config_load is true"
            ),
            "{error}"
        );

        let error = registry()
            .load((uri, [("access_key_id", "test-access")]))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{uri}");
        assert!(
            error.to_string().contains("must be provided together"),
            "{error}"
        );

        registry()
            .load((
                uri,
                [
                    ("access_key_id", "test-access"),
                    ("secret_access_key", "test-secret"),
                ],
            ))
            .unwrap_or_else(|error| panic!("{uri}: {error}"));
    }
}

#[test]
fn provider_builders_require_explicit_credentials_when_config_loading_is_disabled() {
    for (scheme, result) in [
        (
            "tigris",
            Operator::new(Tigris::default().bucket("bucket").disable_config_load()),
        ),
        (
            "r2",
            Operator::new(
                R2::default()
                    .bucket("bucket")
                    .account_id("example-account")
                    .disable_config_load(),
            ),
        ),
        (
            "minio",
            Operator::new(
                Minio::default()
                    .bucket("bucket")
                    .endpoint("https://minio.example.test")
                    .disable_config_load(),
            ),
        ),
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{scheme}");
        assert!(
            error.to_string().contains(
                "access_key_id and secret_access_key are required when disable_config_load is true"
            ),
            "{error}"
        );
    }
}

#[test]
fn minio_uri_can_disable_config_loading_in_anonymous_mode() {
    registry()
        .load("minio://bucket/root?endpoint=https%3A%2F%2Fminio.example.test&skip_signature=true&disable_config_load=true")
        .unwrap();
}

#[test]
fn invalid_provider_connection_options_fail_during_construction() {
    let cases: [(&str, Options<'_>, &str); 13] = [
        ("akamai", &[], "endpoint must be an HTTP(S) base URL"),
        (
            "garage",
            &[("endpoint", "http://localhost:3900")],
            "region must not be empty",
        ),
        (
            "ionos",
            &[("region", "unknown")],
            "region has no documented default endpoint",
        ),
        (
            "neon",
            &[(
                "endpoint",
                "https://proxy.example.com/br-example.storage.c-6.us-east-2.aws.neon.tech",
            )],
            "region is required for a custom endpoint",
        ),
        (
            "rustfs",
            &[
                ("endpoint", "http://localhost:9000"),
                ("secret_access_key", "test-secret"),
            ],
            "must be provided together",
        ),
        (
            "hetzner",
            &[("endpoint", "https://example.test")],
            "region or location is required",
        ),
        (
            "oci",
            &[("region", "us-ashburn-1")],
            "namespace is required",
        ),
        ("spaces", &[], "datacenter is required"),
        (
            "supabase",
            &[("region", "local")],
            "project_ref is required",
        ),
        (
            "wasabi",
            &[("region", "unknown")],
            "region has no documented default endpoint",
        ),
        (
            "exoscale",
            &[("zone", "invalid.zone")],
            "expected a lowercase DNS label",
        ),
        (
            "tigris",
            &[("endpoint", "https://example.test?query=1")],
            "endpoint must be an HTTP(S) base URL",
        ),
        (
            "tigris",
            &[("access_key_id", "test-access")],
            "must be provided together",
        ),
    ];
    for (scheme, options, message) in cases {
        let error = registry()
            .load((format!("{scheme}://bucket/root"), options.iter().copied()))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{scheme}");
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn native_coexisting_presets_reject_incomplete_or_incompatible_options() {
    let cases: &[(&str, Options<'_>, &str)] = &[
        (
            "tos-s3://bucket/root",
            &[("region", "cn-beijing")],
            "endpoint must be an HTTP(S) base URL",
        ),
        (
            "tos-s3://bucket/root",
            &[("endpoint", "https://tos-s3-cn-beijing.volces.com")],
            "region must not be empty",
        ),
        (
            "cos-s3://bucket/root",
            &[("region", "ap-guangzhou")],
            "numeric APPID suffix",
        ),
        (
            "swift-s3api://container/root",
            &[
                ("endpoint", "https://swift.example.com"),
                ("token", "swift-token"),
            ],
            "unknown field `token`",
        ),
        (
            "lakefs-s3://example-repo",
            &[("endpoint", "https://lakefs.example.com")],
            "root must start with a lakeFS branch or reference",
        ),
        (
            "lakefs-s3://example-repo",
            &[
                ("endpoint", "https://lakefs.example.com"),
                ("root", "/./../"),
            ],
            "root must start with a lakeFS branch or reference",
        ),
        (
            "lakefs-s3://example-repo/dev",
            &[
                ("endpoint", "https://lakefs.example.com"),
                ("branch", "main"),
            ],
            "unknown field `branch`",
        ),
        (
            "oss-s3://bucket/root",
            &[
                ("region", "cn-hangzhou"),
                ("enable_virtual_host_style", "false"),
            ],
            "unknown field `enable_virtual_host_style`",
        ),
        (
            "obs-s3://bucket/root",
            &[("endpoint", "https://obs.example.test")],
            "region must not be empty",
        ),
        (
            "obs-s3://bucket/root",
            &[("region", "cn-north-1.example.test")],
            "expected a lowercase DNS label",
        ),
        (
            "obs-s3://bucket/root",
            &[("region", "cn-north-1"), ("session_token", "test-token")],
            "session_token requires both",
        ),
    ];
    for &(uri, options, message) in cases {
        let error = registry().load((uri, options.iter().copied())).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid, "{uri}");
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn explicit_endpoint_configs_reject_unsupported_options_and_redact_credentials() {
    for field in ["role_arn", "enable_virtual_host_style"] {
        let options = [("bucket", "bucket"), (field, "true")];
        let error =
            NeonConfig::from_iter(options.map(|(key, value)| (key.to_owned(), value.to_owned())))
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
        assert!(error.to_string().contains(field));
    }

    let builder = Neon::default()
        .bucket("bucket")
        .access_key_id("test-access")
        .secret_access_key("test-secret");
    let debug = format!("{builder:?}");
    for secret in ["test-access", "test-secret"] {
        assert!(!debug.contains(secret));
    }
}

#[test]
fn preset_configs_reject_aws_only_fields_and_redact_credentials() {
    let error =
        SupabaseConfig::from_iter([("role_arn".to_owned(), "test-role".to_owned())]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    assert!(error.to_string().contains("role_arn"));

    let builder = Supabase::default()
        .bucket("bucket")
        .access_key_id("test-access")
        .secret_access_key("test-secret")
        .session_token("test-user-jwt");
    let debug = format!("{builder:?}");
    for secret in ["test-access", "test-secret", "test-user-jwt"] {
        assert!(!debug.contains(secret));
    }
}
