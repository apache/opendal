
## Compatible Services

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

### Minio

[minio](https://min.io/) is an open-source s3 compatible services.

To connect to minio, we need to set:

- `endpoint`: The endpoint of minio, for example: `http://127.0.0.1:9000`
- `region`: The region of minio. If you don't care about it, just set it to "auto", it will be ignored.
- `bucket`: The bucket name of minio.

```rust,ignore
builder.endpoint("http://127.0.0.1:9000");
builder.region("<region>");
builder.bucket("<bucket_name>");
```

### QingStor Object Storage

[QingStor Object Storage](https://www.qingcloud.com/products/qingstor) is a S3-compatible service provided by [QingCloud](https://www.qingcloud.com/).

To connect to QingStor Object Storage, we need to set:

- `endpoint`: The endpoint of QingStor s3 compatible endpoint, for example: `https://s3.pek3b.qingstor.com`
- `bucket`: The bucket name.

### Scaleway Object Storage

[Scaleway Object Storage](https://www.scaleway.com/en/object-storage/) is a S3-compatible and multi-AZ redundant object storage service.

To connect to Scaleway Object Storage, we need to set:

- `endpoint`: The endpoint of scaleway, for example: `https://s3.nl-ams.scw.cloud`
- `region`: The region of scaleway.
- `bucket`: The bucket name of scaleway.

### Tencent Cloud Object Storage (COS)

[COS](https://intl.cloud.tencent.com/products/cos) is a s3 compatible service provided by [Tencent Cloud](https://intl.cloud.tencent.com/).

To connect to COS, we need to set:

- `endpoint`: The endpoint of cos, for example: `https://cos.ap-beijing.myqcloud.com`
- `bucket`: The bucket name of cos.

### Wasabi Object Storage

[Wasabi](https://wasabi.com/) is a s3 compatible service.

> Cloud storage pricing that is 80% less than Amazon S3.

To connect to wasabi, we need to set:

- `endpoint`: The endpoint of wasabi, for example: `https://s3.us-east-2.wasabisys.com`
- `bucket`: The bucket name of wasabi.

> Refer to [What are the service URLs for Wasabi's different storage regions?](https://wasabi-support.zendesk.com/hc/en-us/articles/360015106031) for more details.

### Cloudflare R2

[Cloudflare R2](https://developers.cloudflare.com/r2/) provides s3 compatible API.

> Cloudflare R2 Storage allows developers to store large amounts of unstructured data without the costly egress bandwidth fees associated with typical cloud storage services.


To connect to r2, we need to set:

- `endpoint`: The endpoint of r2, for example: `https://<account_id>.r2.cloudflarestorage.com`
- `bucket`: The bucket name of r2.
- `region`: When you create a new bucket, the data location is set to Automatic by default. So please use `auto` for region.
- `batch_max_operations`: R2's delete objects will return `Internal Error` if the batch is larger than `700`. Please set this value `<= 700` to make sure batch delete work as expected.
- `enable_exact_buf_write`: R2 requires the non-tailing parts size to be exactly the same. Please enable this option to avoid the error `All non-trailing parts must have the same length`.

### Google Cloud Storage XML API
[Google Cloud Storage XML API](https://cloud.google.com/storage/docs/xml-api/overview) provides s3 compatible API.
- `endpoint`: The endpoint of Google Cloud Storage XML API, for example: `https://storage.googleapis.com`
- `bucket`: The bucket name.
- To access GCS via S3 API, please enable `features = ["native-tls"]` in your `Cargo.toml` to avoid connection being reset when using `rustls`. Tracking in <https://github.com/seanmonstar/reqwest/issues/1809>

### Ceph Rados Gateway
Ceph supports a RESTful API that is compatible with the basic data access model of the Amazon S3 API.

For more information, refer: <https://docs.ceph.com/en/latest/radosgw/s3/>

