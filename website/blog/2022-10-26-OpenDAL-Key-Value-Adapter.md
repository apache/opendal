---
slug: opendal-kay-value-adapter
authors: [Xuanwo]
tags: [OpenDAL]
---
# OpenDAL Key-Value Adapter
OpenDAL has recently added the concept of `Adapter` in its latest version, expanding the range of services supported by OpenDAL from traditional file systems and object storage to Key-Value storage services such as `Redis` and `Moka`. This blog introduces why we added this abstraction, what problems it solves, and the current progress in the community.

## Background
OpenDAL aims to access data freely, painlessly, and efficiently. Freedom means that OpenDAL can access different storage services in the same way. To achieve this goal, OpenDAL supports many services such as azblob, fs, ftp, gcp, hdfs, ipfs, s3 etc., although these services have different interfaces and implementations they all share a commonality which is an abstract class POSIX file system. Therefore, OpenDAL does not need additional abstraction layers to access their data. In other words,the operations on underlying storage are transparent to users with or without using OpenDAL; users can still access these data normally even if they leave out OpenDAL. Based on this design principle ,OpenDAL did not consider supporting Key-Value service because we cannot read any meaningful data from it since its stored data structure highly depends on specific application implementation.

However,in subsequent discussions within our community,it was gradually discovered that defining one's own storage structure based on Key-Value service is also valuable for implementing OpenDAL interface.Databend proposed such a requirement: They hope that OpenDAL can implement a layer of cache that stores data in different caching services,such as memory, local file system,and Redis.They want to set certain eviction policies so that cached items will be automatically expired.In other words,Databend hopes that OpenDAL can support volatile-data-storage which will be used for caching or temporary purposes.Users won't use other ways (such as awscli)to access them.In this scenario,the key-value service is like a black box,and users only use OpenDAL to access it.Since there is no need to expose external accesses.OpenDAL can fully decide how its stored structure should look like based on key-value service.

## Design
In the past,the implementation path of open Dal was very simple:

```rust
impl Accessor for s3::Backend { .. }
```

Each service provides a Backend struct and implements Accessor interface.

But since interfaces of Key-Value Services are very similar,to avoid duplicating similar logic.OpenDAL introduced Adapter:

```rust
impl<S> Accessor for kv::Backend<S> where S: kv::Adapter { .. }

impl kv::Adapter for redis::Adapter { .. }
```

OpenDAL implemented Accessor for all `kv::Backend <S:kv::Adapter>`,and each concrete key-value Service only needs to implement `kv::Adapter`:

```rust
#[async_trait]
pub trait Adapter: Send + Sync + Debug + Clone + 'static {
    /// Return the medata of this key value accessor.
    fn metadata(&self) -> Metadata;

    /// Fetch the next id.
    ///
    /// - Returning id should never be zero.
    /// - Returning id should never be reused.
    async fn next_id(&self) -> Result<u64>;

    /// Get a key from service.
    ///
    /// - return `Ok(None)` if this key is not exist.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a key into service.
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Scan a range of keys.
    ///
    /// If `scan` is not supported, we will disable the block split
    /// logic. Only one block will be store for one file.
    async fn scan(&self, prefix: &[u8]) -> Result<KeyStreamer> {
        let _ = prefix;

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            anyhow::anyhow!("scan operation is not supported"),
        ))
    }

    /// Delete a key from service.
    ///
    /// - return `Ok(())` even if this key is not exist.
    async fn delete(&self, key: &[u8]) -> Result<()>;
}
```

Here,`get`,`set`,and `delete` are interfaces supported by most Key-Value Services.Moreover,`kv::Adapter` will decide internal implementation details according whether scan operation is supported.The specific format has not been completely stabilized yet.So I won't go into further detail here.Interested students may directly check out code~

Progress
Based on `kv::Adapter.OpenDAL` has implemented support for following Key Value Services:

- [memory](https://opendal.databend.rs/opendal/services/struct.Memory.html): A Service based on BtreeMap

- [moka](https://opendal.databend.rs/opendal/services/struct.Moka.html): A Service based high-performance cache library [moka](https://github.com/moka-rs/moka)

- [redis](https://opendal.databend.rs/opendal/services/struct.Redis.html): A Service based [redis](https://redis.io/)

Next,Databend community will try using these Services as cache layer accelerate hot queries.And improve OpenDAL's implementation according feedbacks received during production~

The community also plans add support following KV-Services:
- [tikv](https://github.com/datafuselabs/opendal/issues/854)
- [rocksdb](https://github.com/datafuselabs/opendal/issues/855)
- [memcached](https://github.com/datafuselabs/opendal/issues/856)
