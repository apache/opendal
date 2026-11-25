## Compatible Services

OpenDAL provides provider presets for services with a maintained, narrow
configuration contract:

- Use [`crate::R2`] or the `r2` scheme for Cloudflare R2.
- Use [`crate::Minio`] or the `minio` scheme for MinIO deployments.
- Use [`crate::Akamai`] or the `akamai` scheme for Akamai Cloud Object Storage.
- Use [`crate::CephRgw`] or the `ceph-rgw` scheme for Ceph RADOS Gateway.
- Use [`crate::Exoscale`] or the `exoscale` scheme for Exoscale Simple Object Storage.
- Use [`crate::Garage`] or the `garage` scheme for Garage deployments.
- Use [`crate::Hetzner`] or the `hetzner` scheme for Hetzner Object Storage.
- Use [`crate::IbmCos`] or the `ibm-cos` scheme for IBM Cloud Object Storage.
- Use [`crate::IdriveE2`] or the `idrive-e2` scheme for IDrive e2.
- Use [`crate::Ionos`] or the `ionos` scheme for IONOS Cloud Object Storage.
- Use [`crate::Neon`] or the `neon` scheme for Neon Object Storage.
- Use [`crate::Oci`] or the `oci` scheme for Oracle Cloud Infrastructure Object Storage.
- Use [`crate::Ovhcloud`] or the `ovhcloud` scheme for OVHcloud Object Storage.
- Use [`crate::Qingstor`] or the `qingstor` scheme for QingStor Object Storage.
- Use [`crate::Rustfs`] or the `rustfs` scheme for RustFS deployments.
- Use [`crate::Scaleway`] or the `scaleway` scheme for Scaleway Object Storage.
- Use [`crate::Seaweedfs`] or the `seaweedfs` scheme for SeaweedFS S3 gateways.
- Use [`crate::Spaces`] or the `spaces` scheme for DigitalOcean Spaces.
- Use [`crate::Storj`] or the `storj` scheme for the Storj-hosted S3 Gateway.
- Use [`crate::Supabase`] or the `supabase` scheme for Supabase Storage.
- Use [`crate::Tigris`] or the `tigris` scheme for Tigris.
- Use [`crate::Wasabi`] or the `wasabi` scheme for Wasabi Object Storage.

The presets own provider defaults and validation. Their capabilities describe the
shared S3 implementation. Check the provider guides for endpoint-specific limits.
Presets built on the shared credential chain accept a provider's key pair
explicitly or load it from the standard AWS environment variables or shared
credentials file. Explicit credentials take precedence.
Use the generic [`crate::S3`] builder for AWS S3, for compatible services without
a built-in preset, or when an application requires S3 options outside a preset's
contract.

### AWS S3

[AWS S3](https://aws.amazon.com/s3/) is the default implementations of s3 services. Only `bucket` is required.

```rust,ignore
builder.bucket("<bucket_name>");
```

### Native services and S3 presets

The facade registers the native services under their existing schemes. The
S3 presets use separate schemes and their own Cargo features. Enable the
corresponding preset feature to select an S3 API explicitly.

- Use [`crate::B2S3`] or the `b2-s3` scheme for Backblaze B2.
- Use [`crate::CosS3`] or the `cos-s3` scheme for Tencent Cloud COS.
- Use [`crate::GcsS3`] or the `gcs-s3` scheme for Google Cloud Storage.
- Use [`crate::LakefsS3`] or the `lakefs-s3` scheme for the lakeFS S3 Gateway.
- Use [`crate::OssS3`] or the `oss-s3` scheme for Alibaba Cloud OSS.
- Use [`crate::SwiftS3api`] or the `swift-s3api` scheme for OpenStack Swift s3api.
- Use [`crate::TosS3`] or the `tos-s3` scheme for Volcengine and BytePlus TOS.

Applications can override a scheme in the process-wide operator registry.
For example, enable `services-gcs-s3` in the facade and register `GcsS3`
for `gcs` after initializing the default registry.

```rust,ignore
use opendal::{Operator, OperatorRegistry, services::GcsS3};

opendal::init_default_registry();
OperatorRegistry::get().register::<GcsS3>("gcs");
let op = Operator::from_uri("gcs://example-bucket/prefix")?;
```

After an override, URIs use the selected preset's fields and URI syntax.
In particular, the Swift and lakeFS presets use the URI authority for the
bucket and take the gateway URL in the `endpoint` option. The native Swift
and lakeFS schemes use the authority for the endpoint.

### Copy with metadata replacement

`copy_with_user_metadata`, `copy_with_content_type` and `copy_with_content_language` are enabled for every endpoint. They send `CopyObject` with `x-amz-metadata-directive: REPLACE`, and a multipart copy sets the metadata on `CreateMultipartUpload`.

The behavior tests for these options pass on AWS S3, Garage v2.1.0, SeaweedFS 4.52 and Silo. No known service rejects them so far. The other services in this list, and S3 Express One Zone directory buckets, are not verified. If a service ignores or rejects the directive, override the three capabilities to `false`:

```rust,ignore
let op = Operator::new(builder)?.layer(CapabilityOverrideLayer::new(|mut cap| {
    cap.copy_with_user_metadata = false;
    cap.copy_with_content_type = false;
    cap.copy_with_content_language = false;
    cap
}));
```
