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

use opendal_core::Configurator;
use opendal_core::ErrorKind;
use opendal_core::OperatorRegistry;
use url::Url;

use super::*;

type Options<'a> = &'a [(&'a str, &'a str)];

fn registry() -> &'static OperatorRegistry {
    let registry = OperatorRegistry::get();
    for register in [
        register_exoscale_service as fn(&OperatorRegistry),
        register_hetzner_service,
        register_oci_service,
        register_ovhcloud_service,
        register_qingstor_service,
        register_scaleway_service,
        register_spaces_service,
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
    assert_eq!(op.info().scheme(), uri.split_once(':').unwrap().0);
    assert_eq!(op.info().root(), "/root/");

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
    let cases: [(&str, Options<'_>, &str, &str, &str); 10] = [
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
    ];

    for (uri, options, expected_host, expected_path, expected_region) in cases {
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
            "wasabi://bucket/root",
            &[("region", "us-east-1")][..],
            "us-east-1",
        ),
    ] {
        let mut options = options.to_vec();
        options.push(("session_token", "test-token"));
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

#[test]
fn invalid_provider_connection_options_fail_during_construction() {
    let cases: [(&str, Options<'_>, &str); 8] = [
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
