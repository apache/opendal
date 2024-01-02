- Proposal Name: `concurrent_writer`
- Start Date: 2024-01-02
- RFC PR: [apache/incubator-opendal#3898](https://github.com/apache/incubator-opendal/pull/3898)
- Tracking Issue: [apache/incubator-opendal#3899](https://github.com/apache/incubator-opendal/issues/3899)

# Summary

Add concurrent write in `MultipartUploadWriter`.

# Motivation

The [object_writer](./1420_object_writer.md) introduces the `ObjectWriter` multipart upload support. However, the multiple parts are currently uploaded serially without fully leveraging the potential for improved throughput through concurrent uploads. We should support the upload of multiple parts concurrently.

# Guide-level explanation

For users who want to concurrent writer, they will call the new API `concurrent`. And the default behavior remains unchanged, so users using `op.writer_with()` are not affected. The `concurrent` function will take a number as input, and this number will represent the maximum concurrent write task amount the writer can perform.

```rust
op.writer_with(path).concurrent(8).await
```

# Reference-level explanation

This feature will be implemented in the `MultipartUploadWriter`, which will utilize a `ConcurrentFutures<WriteTask>` as a task queue to store concurrent write tasks.

A `concurrent` field of type `usize` will be introduced to `OpWrite`. If `concurrent` is set to 0 or 1, it functions with default behavior. However, if concurrent is set to number larger than 1, it denotes the maximum concurrent write task amount that the `MultipartUploadWriter` can utilize. 

When the upper layer invokes `poll_write`, the  `MultipartUploadWriter` pushes `concurrent` upload parts to the task queue (`ConcurrentFutures<WriteTask>`) if there are available slots. If the task queue is full, the `MultipartUploadWriter` waits for the first task to yield results.

# Drawbacks

None

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

None

# Future possibilities

None
