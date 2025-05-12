## MokaConfig

### `name`: `str`

Name for this cache instance.

### `max_capacity`: `_int`

Sets the max capacity of the cache.

Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)

### `time_to_live`: `_duration`

Sets the time to live of the cache.

Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)

### `time_to_idle`: `_duration`

Sets the time to idle of the cache.

Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)

### `num_segments`: `_int`

Sets the segments number of the cache.

Refer to [`moka::sync::CacheBuilder::segments`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.segments)

### `root`: `str`

root path of this backend

