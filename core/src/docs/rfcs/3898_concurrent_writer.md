- Proposal Name: `concurrent_writer`
- Start Date: 2024-01-02
- RFC PR: [apache/incubator-opendal#3898](https://github.com/apache/incubator-opendal/pull/3898)
- Tracking Issue: [apache/incubator-opendal#3899](https://github.com/apache/incubator-opendal/issues/3899)

# Summary

Enhance the `Writer` by adding concurrent write capabilities.

# Motivation

Currently, When invoking `writer` or `writer_with` on an `Operator` that utilizes Object storage as its backend, such as S3, the `Operator` will return a `Writer` that supports multipart uploading. 

```rust
    // The `op` is a S3 backend `Operator`.
    let mut writer = op.writer("path/to").await?;
    writer.write(part0).await?;
    writer.write(part1).await?; // It starts to upload after the `part0` is finished.
```
However, the multiple parts are currently uploaded serially without fully leveraging the potential for improved throughput through concurrent uploads. We should support the upload of multiple parts concurrently.


# Guide-level explanation

For users who want to concurrent writer, they will call the new API `concurrent`. And the default behavior remains unchanged, so users using `op.writer_with()` are not affected. The `concurrent` function will take a number as input, and this number will represent the maximum concurrent write task amount the writer can perform.

- If `concurrent` is set to 0 or 1, it functions with default behavior(Uploads parts serially). 
- However, if `concurrent` is set to number larger than 1. It enables concurrent uploading of up to `concurrent` write tasks and allows users to initiate additional write tasks without waiting to complete the previous part uploading, as long as the inner task queue still has available slots.

It won't interact with other existing components, except the `buffer` inside a `Writer`. If the multipart upload isn't initialized, `Writer` puts the bytes into `buffer` first, then retrieves it back when uploading the part.

```rust
op.writer_with(path).concurrent(8).await
```

# Reference-level explanation

The concurrent write capability is only supported for services that have implemented the `MultipartUploadWriter` or `RangeWriter`. Otherwise, setting the `concurrent` parameter will have no effect (Same as default behavior). 

This feature will be implemented in the `MultipartUploadWriter` and `RangeWriter`, which will utilize a `ConcurrentFutures<WriteTask>` as a task queue to store concurrent write tasks. A `concurrent` field of type `usize` will be introduced to `OpWrite` to allow the user setting the maximum concurrent write task amount.

When the upper layer invokes `poll_write`, the  `Writer` pushes write to the task queue (`ConcurrentFutures<WriteTask>`) if there are available slots, and then relinquishes control back to the upper layer. This allows for up to `concurrent` write tasks to uploaded concurrently without waiting. If the task queue is full, the `Writer` waits for the first task to yield results.

In the future, we can introduce the `write_at` for `fs` and use `ConcurrentFutureUnordered` instead of `ConcurrentFutures.`.

# Drawbacks

- More memory usage
- More concurrent connections

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

None

# Future possibilities

- Adding `write_at` for `fs`.
- Add `ConcurrentFutureUnordered`
