# Apache OpenDAL™ Checksum Layer

`ChecksumLayer` requests native checksums on writes and verifies stored
checksums on single-response whole-object reads. Attach the layer with
`Operator::layer(ChecksumLayer::new())` to enable both in best-effort mode.

The default write policy is `WriteChecksumPolicy::Fast`. An explicit per-write
checksum takes precedence. Best-effort writes use the original options if the
service lacks checksum support or rejects the injected policy before returning
a writer. Set `require_write(true)` to require support for the configured policy,
or `write(None)` to disable write policy injection.

Set `require_read(true)` to require a supported checksum on whole-object reads.
`verify_read(false)` disables read verification and automatic read checksum
requests independently of the write policy.

On services that advertise read checksum support, verification requests the
checksum on unbounded and nonempty bounded forward reads starting at offset zero.
This includes prefix reads and the first chunk of a chunked read. The layer
preserves the requested range. A bounded read is verified only when its own
response metadata shows that it covers the whole object.
