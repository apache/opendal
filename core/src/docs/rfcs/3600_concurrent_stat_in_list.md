- Proposal Name: `concurrent_stat_in_list`
- Start Date: 2023-09-16
- RFC PR: [apache/incubator-opendal#3526](https://github.com/apache/incubator-opendal/pull/)
- Tracking Issue: [apache/incubator-opendal#3097](https://github.com/apache/incubator-opendal/issues/3097)

# Summary

Add concurrent stat in list operation.

# Motivation

[RFC-2779](https://github.com/apache/incubator-opendal/pull/2779) allows user to list with metakey.
However, the stat inside list could make the list process much slower. We should allow concurrent stat during list so that stat could be sent concurrently.


# Guide-level explanation

For users who want to concurrently run statistics in list operations, they will call the new API `concurrent`. The `concurrent` function will take a number as input, and this number will represent the maximum concurrent stat handlers.

The default behavior remains unchanged, so users using `op.list_with()` are not affected. And this implementation should be zero cost to users who don't want to do concurrent stat.

```rust
op.lister_with(path).metakey(Metakey::ContentLength).concurrent(10).await?
```


# Reference-level explanation

When `concurrent` is set and `list_with` is called, the list operation will be split into two parts: list and stat.
The list part will iterate through the entries inside the buffer, and if its metakey is unknown, it will send a stat request to the storage service.

We will add a new field `concurrent` to `OpList`. The type of `concurrent` is `Option<usize>` (TBD). If `concurrent` is `None`, it means the default behavior. If `concurrent` is `Some(n)`, it means the maximum concurrent stat handlers are `n`.

Then we could use a `Semaphore` (or similar) to limit the maximum concurrent stat handlers. Additionally, we could use handlers (`JoinSet` or `Vec<JoinHandle>`, etc.) to spawn and stack the stat task. While iterating through the entries, we should check if the metakey is unknown and if the `Semaphore` is available. If the metakey is unknown and the `Semaphore` is available, we should spawn a new stat handler and push it into the handlers stack.

If the metakey is unknown and the handlers are full, we should break the loop and wait for the spawned tasks inside handlers to finish. After the spawned tasks finish, we should iterate through the handlers and return the result.

If the metakey is known, we should return the result immediately and go back to return the concurrent tasks' results after the spawned tasks finish.

# Drawbacks

We never support this feature before.

# Rationale and alternatives

None

# Prior art

None

# Unresolved questions

1. Can we avoid using `Semaphore` and `JoinSet`, which introduce extra costs? Could we store those features directly?

2. Is this implementation zero cost to users who don't want to do concurrent stat?

3. How to implement a similar logic to `blocking` API?

# Future possibilities

None
