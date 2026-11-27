# Checksums

A full-object checksum identifies an algorithm and the raw digest of the
object's complete byte sequence. CRC digests use big-endian byte order. The
checksum field in metadata is reserved for full-object digests. Option and
capability availability depends on the service and language binding.

## Write intents

An explicit write checksum requires verification by the service.

| Intent | Required verification |
| --- | --- |
| `Any` | Use a native write checksum, preferring a CRC when available. |
| `Fast` | Use a native CRC checksum. |
| `Algo(algorithm)` | Compute a full-object digest with the requested algorithm and verify it before commit. |
| `Expected(checksum)` | Verify the complete object against the caller's digest before commit. |

`Any` and `Fast` allow the service to verify each upload request, part, or
block independently. The successful write result carries a checksum when the
service verified a full-object digest. It can have no checksum when every
request was verified independently. `Algo` and `Expected` always return the
verified full-object digest on success.

`write_with_checksum` guarantees support for at least `Any` on supported
non-append writes. Other intents, algorithms, and combinations depend on the
service. Append can use `Any` or `Fast` where supported. OpenDAL rejects an
unsupported explicit intent, including `Algo` or `Expected` on append, with
`Unsupported` before I/O. An `Expected` digest of the wrong length fails with
`ConfigInvalid` before I/O.

## Write failures and visibility

- A checksum rejection before commit leaves the previous object intact for a
  replacement and the key absent for a creation. OpenDAL returns
  `ChecksumMismatch` when it recognizes a pre-commit checksum rejection. An
  HTTP success status can carry such a rejection in the response body. A
  rejection of a computed digest can be temporary when OpenDAL can safely
  replay the request with the original bytes. A final mismatch against the
  caller's `Expected` digest or a deterministic completion rejection is
  non-temporary.
- OpenDAL propagates a service failure it cannot identify as a checksum
  rejection. The resulting error may be a non-temporary `Unexpected`.
- A missing, invalid, or mismatched receipt can prevent OpenDAL from confirming
  required verification after commit. This returns a non-temporary `Unexpected`.
  The new or replacement data may already be visible.
- When an append commits each block independently, a pre-commit rejection
  prevents that block from being appended. Earlier successful blocks stay
  committed after a later failure or an abort. A block with an unconfirmed
  post-commit receipt may also be visible.

An `Unexpected` error alone leaves the commit status unknown.

## Retrieving checksum metadata

The `checksum` option on stat, read, and reader operations requests stored
full-object checksum metadata. It defaults to `false`. Services can supply the
metadata by default or complete the operation without a digest, including
when they cannot honor the hint. Stat retrieves metadata without reading
object data to calculate a digest.

A digest returned on a read describes the complete object in the byte
representation being read. A range response can carry that same full-object
metadata. Retrieving the digest alone leaves verification of the bytes to a
separate step.

## Verifying reads

When enabled, `ChecksumLayer` verifies a supported stored checksum when a
single response covers the whole object. An unbounded read from offset zero
qualifies. A bounded read from zero or a suffix read also qualifies when its
response reports the complete object length and the requested size covers
that length. For example, an HTTP partial-content response can cover the
entire object.

On services advertising `read_with_checksum`, the layer requests the digest
for unbounded and nonempty bounded reads from offset zero. Prefix reads and
the first request of a chunked read can therefore request the digest. The
layer preserves the range and any caller-requested checksum hint.

By default, a whole-object read can succeed without a supported checksum.
With `require_read(true)`, it fails with `Unsupported` before returning bytes
if the digest is absent or unsupported. Partial ranges, reads of unknown
coverage, and reads assembled from multiple partial responses are exempt
from verification and this requirement.

A digest mismatch during verification returns a non-temporary
`ChecksumMismatch`. A verifying stream checks the digest before yielding the
buffer that completes the known object length, or at end of stream if the
length is unknown. Earlier buffers can already have reached the caller.
The verifier returns the same failure on subsequent polls of that stream.

## Copying through a writer

An explicit write checksum makes `Writer::copy_from` stream the source bytes
through the client and upload them as part of the write. The destination
verification covers those bytes alongside any other data in the write.

If a streaming copy reports a source `ChecksumMismatch` after the writer has
accepted any bytes from that logical copy, the writer requires `abort()`.
Progress includes queued bytes and bytes accepted on earlier attempts of the
same copy. Further writes, copies, and close fail. With zero progress on that
copy, the writer remains usable. A successful close without more input commits
only the previously accepted data. Ordinary temporary read errors can still
resume the same copy.

## Historical decisions

- [RFC 9166: Checksum Intent and Layer](https://github.com/apache/opendal/blob/main/core/core/src/docs/rfcs/9166_checksum_intent_and_layer.md)
