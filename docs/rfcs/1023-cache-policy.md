- Proposal Name: `cache-policy`
- Start Date: 2022-11-30
- RFC PR: [datafuselabs/opendal#1023](https://github.com/datafuselabs/opendal/pull/1023)
- Tracking Issue: [datafuselabs/opendal#846](https://github.com/datafuselabs/opendal/issues/846)

# Summary

Provide the cache layer with the ability to load different caching policies.

# Motivation

Currently our caching layer only supports basic reads and writes, and the caching policy relies on the underlying storage.

This means that if you use `moka` as a cache backend:

- Admission to a cache is controlled by the Least Frequently Used (LFU) policy.
- Eviction from a cache is controlled by the Least Recently Used (LRU) policy.

However, if you use other services as backend, like `fs`. As these services themselves do not contain a cache policy, we cannot benefit from them.

To solve this problem, a plain idea is to allow setting the caching policy for the `CacheLayer`.

# Guide-level explanation

`CacheLayer` will take two additional parameters, `admit_policy` and `evict_policy` , to specify the insertion and deletion strategy.

```diff
- CacheLayer::new(Operator::from_env(Scheme::Memory).expect("must init"),CacheStrategy::Whole,)
+ CacheLayer::new(Operator::from_env(Scheme::Memory).expect("must init"),CacheStrategy::Whole,AlwaysAdmitPolicy, LruEvictPolicy)
```

`AlwaysAdmitPolicy` and `LruEvictPolicy` are implementations of the `trait`s `AdmitPolicy` and `EvictPolicy` respectively. Of course, users can implement the corresponding policies to suit their needs.

# Reference-level explanation

```diff
#[derive(Debug, Clone)]
pub struct CacheLayer {
    cache: Arc<dyn Accessor>,
+   admit_policy: Arc<dyn AdmitPolicy>,
+   evict_policy: Arc<dyn EvictPolicy>,
    strategy: CacheStrategy,
}
```

The CacheLayer will contain two new fields, `admit_policy` and `evict_policy`, corresponding to the implementation of the traits `AdmitPolicy` and `EvictPolicy` respectively.

The policies is lighter than a specific cache implementation, as it only needs to maintain keys and leave the values to the services backends.

Since the cache's key is the path, we do not need to consider more complex cases.

```rust
pub trait AdmitPolicy {
    fn should_add(&mut self, key: &str) -> bool;
    fn should_replace(&mut self, admission: &str, eviction: &str) -> bool;
    fn on_hit(&mut self, key: &str);
    fn on_miss(&mut self, key: &str);
    fn clear(&mut self);
    fn remove(&mut self, key: &str);
}
```

```rust
pub trait EvictPolicy {
    fn peek_eviction(&mut self) -> Option<str>;
    fn on_evict(&mut self, key: &str);
    fn on_insert(&mut self, key: &str);
    fn on_update(&mut self, key: &str);
    fn on_hit(&mut self, key: &str);
    fn clear(&mut self);
    fn remove(&mut self, key: &str);
}
```

```rust
pub trait EvictPolicy {
    fn peek_eviction(&mut self) -> Option<str>;
    fn on_evict(&mut self, key: &str);
    fn on_insert(&mut self, key: &str);
    fn on_update(&mut self, key: &str);
    fn on_hit(&mut self, key: &str)
    fn clear(&mut self);
    fn remove(&mut self, key: &str)
}
```

# Drawbacks

None.

# Rationale and alternatives

None.

# Prior art

## Lfan

[lfan](https://crates.io/crates/lfan) has implemented this solution in Rust, providing a modular cache.

However, it considers the more generic case and we need to make some modifications to suit the opendal reality.

# Unresolved questions

None.

# Future possibilities

None.
