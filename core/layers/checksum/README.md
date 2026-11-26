# Apache OpenDAL™ Checksum Layer

`ChecksumLayer` verifies stored checksums for single-response whole-object reads.
Attach the layer with `Operator::layer(ChecksumLayer::new())` for best-effort
verification, or set `require_read(true)` to require a supported checksum on
those reads.

On services that advertise read checksum support, verification requests the
checksum on unbounded and nonempty bounded forward reads starting at offset zero.
This includes prefix reads and the first chunk of a chunked read. The layer
preserves the requested range. A bounded read is verified only when its own
response metadata shows that it covers the whole object.
