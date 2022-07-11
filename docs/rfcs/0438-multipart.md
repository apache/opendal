- Proposal Name: `multipart`
- Start Date: 2022-07-11
- RFC PR: [datafuselabs/opendal#438](https://github.com/datafuselabs/opendal/pull/438)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Add multipart support in OpenDAL.

# Motivation

[Multipart Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) APIs are widely used in object storage services to upload large files concurrently and resumable.

A successful multipart upload includes the following steps:

- `CreateMultipartUpload`: Start a new multipart upload.
- `UploadPart`: Upload a single part with the previously got upload id.
- `CompleteMultipartUpload`: Complete a multipart upload to get a regular object.

To cancel a multipart upload, users need to call `AbortMultipartUpload`.

Apart from those APIs, most object services also provide a list API to get the current multipart uploads status:

- `ListMultipartUploads`: List current ongoing multipart uploads
- `ListParts`: List already uploaded parts.

Before `CompleteMultipartUpload` has been called, users can't read already uploaded parts.

After `CompleteMultipartUpload` or `AbortMultipartUpload` has been called, all uploaded parts will be removed.

Object storage services commonly allow 10000 parts, and every part will allow up to 5 GiB. This way, users can upload a file up to 48.8 TiB.

OpenDAL users can upload objects larger than 5 GiB via supporting multipart uploads.

# Guide-level explanation

TBD

# Reference-level explanation

TBD
# Drawbacks

TBD

# Rationale and alternatives

TBD

# Prior art

TBD

# Unresolved questions

TBD

# Future possibilities

TBD
