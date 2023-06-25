# Chapter-02: Async IO

In [Chapter-01](../01-init-operator/README.md), we learnt how to initiate an `Operator`. All operations in OpenDAL is IO bound which means most of our users will use OpenDAL in an async runtime. This chapter will discuss how to call `Operator` in async runtime and how to use OpenDAL to read and write data!

## Warmup

Before delving into the OpenDAL API, let's take a look at some new Rust features.

### Crate Features

In this chapter's `Cargo.toml`, we add a new dependence `tokio`:

```toml
tokio = { version = "1", features = ["full"] }
```

The syntex is different from what we used before:

```diff
- tokio = "1"
+ tokio = { version = "1", features = ["full"] }
```

We use this syntax to specify additional options for this dependency. Essentially, `tokio = "1"` can be considered a shorthand for `tokio = { version = "1" }`.

The most commonly used option is `features`, which works in conjunction with conditional compilation and the crate's configuration system. These features allow you to enable or disable specific pieces of code within the crate based on different use cases.

For instance, since the `tokio` project has many components, not all of them are necessary for every project. By enabling only the required features, we can reduce build time.

In our examples, we will just enable the `macros` and `"rt-multi-thread` feature which allow use to use `#[tokio::main]` in code.

### Attributes

Let's back to read our `main.rs`. We will find a new thing here: `#[tokio::main]`.
