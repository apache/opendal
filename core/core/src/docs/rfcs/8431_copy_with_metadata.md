- Proposal Name: `copy_with_metadata`
- Start Date: 2026-11-21
- RFC PR: [apache/opendal#8431](https://github.com/apache/opendal/pull/8431)
- Tracking Issue: [apache/opendal#8430](https://github.com/apache/opendal/issues/8430)

# Summary

Add `user_metadata`, `content_type`, and `content_language` to `CopyOptions`.
When any of them is set, a copy replaces the destination's whole metadata set
with the given values. The same options allow a copy onto its own path, which
changes a file's metadata without rewriting its content.

# Motivation

A metadata-only change, such as setting a file's modification time after an
upload, costs a full read and rewrite today. `copy` cannot carry new metadata
and rejects `from == to`. S3 performs this change with one `CopyObject` request
that sends `x-amz-metadata-directive: REPLACE` and transfers no content.

# Guide-level explanation

```rust,ignore
op.copy_with("photo.jpg", "photo.jpg")
    .user_metadata([("mtime".to_string(), "1700000000".to_string())])
    .content_type("image/jpeg")
    .await?;
```

The call stores exactly this metadata on `photo.jpg`. Metadata that the call
does not pass, including user metadata the file had before, is gone. A caller
that changes one field passes the complete target set. Only `user_metadata`,
`content_type`, and `content_language` can be passed back this way; other
source headers such as Cache-Control, Content-Disposition, and
Content-Encoding cannot be preserved through this API and reset to the service
default under S3 `REPLACE`.

# Reference-level explanation

## Options and capabilities

| Option | Capability |
| --- | --- |
| `user_metadata: Option<HashMap<String, String>>` | `copy_with_user_metadata` |
| `content_type: Option<String>` | `copy_with_content_type` |
| `content_language: Option<String>` | `copy_with_content_language` |

`copy_with` and `copier_with` expose matching builder methods, and `OpCopy`
exposes matching getters. All capabilities default to `false`.
`Operator` returns `Unsupported` for an option whose capability the operator's
effective capability does not have, before the service sends any request. The
check runs against the effective capability, so `CapabilityOverrideLayer` can
turn each flag off or on.

## Whole-set replacement

When any of the three options is set, the destination inherits no metadata
from the source. It gets exactly the given values, and every unset field takes
the service default. This includes the unset options themselves and other
system metadata such as Cache-Control. Without these options, copy keeps its
current behavior.

## Same-path copy

`Operator::copy*` returns `IsSameFile` for `from == to` unless the call sets
`source_version` or one of the three options. A same-path copy with a
replacement option keeps the content and replaces the metadata. If it also sets
`source_version`, the destination receives the bytes of that source version,
not necessarily the current object's bytes.

## Multipart copy

A service that splits a copy into several server-side requests applies the
metadata when it creates the destination. S3 sets `Content-Type`,
`Content-Language`, and `x-amz-meta-*` on `CreateMultipartUpload`; the part
copies carry no metadata.

## S3 mapping

S3 `CopyObject` sends `x-amz-metadata-directive: REPLACE` together with
`Content-Type`, `Content-Language`, and `x-amz-meta-*` for the given values.
Without REPLACE, S3 copies the source metadata and ignores these headers.

## Providers

A service advertises a capability only when it honors the option on
single-request, multipart, and same-path copy. Following the
maximum-capability default, `s3` enables the three capabilities for every
endpoint, including S3 Express One Zone. The `s3` behavior tests for these
options pass against AWS S3, Garage v2.1.0, Silo, and SeaweedFS 4.52. The s3
`compatible_services.md` lists the verified providers and shows how to turn the
capabilities off with `CapabilityOverrideLayer` for a provider that does not
honor them.

# Drawbacks

A caller who expects a patch loses the fields it did not pass. The options
document this at the API site.

# Rationale and alternatives

Preserving unset fields would need a `stat` before the copy and a merge. That
is a second request and races with concurrent writers; S3 has no partial
metadata update. Callers that already hold the full set, such as a file server
answering `SETSTAT` or `PROPPATCH`, pass it directly.

A separate `update_metadata` operation would map to the same S3 request and
duplicate the copy path, including multipart copy and conditions.

# Prior art

S3 `CopyObject` with `x-amz-metadata-directive: REPLACE`. GCS `rewrite` and
Azure Blob `Copy Blob` also accept destination metadata on copy.

# Unresolved questions

The other S3-compatible providers and S3 Express One Zone are not verified yet.
Per-provider capability presets could replace the documented override once the
`s3` service has them.

# Future possibilities

Add Cache-Control, Content-Disposition, and Content-Encoding to the replacement
set when a user needs them.
