# Presign

OpenDAL can presign an operation to generate a presigned URL.

![](../assets/rfcs/0413-presign/process.png)

Refer to [RFC-0413: Presign](/rfcs/0413-presign.html) for more information.

## Download

```rust
let op = Operator::from_env(Scheme::S3).await?;
let signed_req = op.object("test").presign_read(Duration::hours(1))?;
```

- `signed_req.method()`: `GET`
- `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
- `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`

We can download this object via `curl` or other tools without credentials:

```shell
curl "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -O /tmp/test.txt
```

## Upload

```rust
let op = Operator::from_env(Scheme::S3).await?;
let signed_req = op.object("test").presign_write(Duration::hours(1))?;
```

- `signed_req.method()`: `PUT`
- `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
- `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`

We can upload file as this object via `curl` or other tools without credential:

```shell
curl -X PUT "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -d "Hello, World!"
```
