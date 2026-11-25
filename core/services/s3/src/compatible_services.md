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
Use the generic [`crate::S3`] builder for AWS S3, for compatible services without
a built-in preset, or when an application requires S3 options outside a preset's
contract.

### AWS S3

[AWS S3](https://aws.amazon.com/s3/) is the default implementations of s3 services. Only `bucket` is required.

```rust,ignore
builder.bucket("<bucket_name>");
```

### Alibaba Object Storage Service (OSS)

[OSS](https://www.alibabacloud.com/product/object-storage-service) is a s3 compatible service provided by [Alibaba Cloud](https://www.alibabacloud.com).

To connect to OSS, we need to set:

- `endpoint`: The endpoint of oss, for example: `https://oss-cn-hangzhou.aliyuncs.com`
- `bucket`: The bucket name of oss.

> OSS provide internal endpoint for used at alibabacloud internally, please visit [OSS Regions and endpoints](https://www.alibabacloud.com/help/en/object-storage-service/latest/regions-and-endpoints) for more details.

> OSS only supports the virtual host style, users could meet errors like:
>
> ```xml
> <?xml version="1.0" encoding="UTF-8"?>
> <Error>
>  <Code>SecondLevelDomainForbidden</Code>
>  <Message>The bucket you are attempting to access must be addressed using OSS third level domain.</Message>
>  <RequestId>62A1C265292C0632377F021F</RequestId>
>  <HostId>oss-cn-hangzhou.aliyuncs.com</HostId>
> </Error>
> ```
>
> In that case, please enable virtual host style for requesting.

```rust,ignore
builder.endpoint("https://oss-cn-hangzhou.aliyuncs.com");
builder.region("<region>");
builder.bucket("<bucket_name>");
builder.enable_virtual_host_style();
```

### Tencent Cloud Object Storage (COS)

[COS](https://intl.cloud.tencent.com/products/cos) is a s3 compatible service provided by [Tencent Cloud](https://intl.cloud.tencent.com/).

To connect to COS, we need to set:

- `endpoint`: The endpoint of cos, for example: `https://cos.ap-beijing.myqcloud.com`
- `bucket`: The bucket name of cos.

### Google Cloud Storage XML API
[Google Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview) provides s3 compatible API.
- `endpoint`: The endpoint of Google Cloud Storage XML API, for example: `https://storage.googleapis.com`
- `bucket`: The bucket name.
- To access GCS via S3 API, please enable `features = ["native-tls"]` in your `Cargo.toml` to avoid connection being reset when using `rustls`. Tracking in <https://github.com/seanmonstar/reqwest/issues/1809>

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
