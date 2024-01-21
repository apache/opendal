# Chapter-02: Async IO

In [Chapter-01](../01-init-operator/README.md), we learnt how to initiate an `Operator`. All operations in OpenDAL is IO bound which means most of our users will use OpenDAL in an async runtime. This chapter will discuss how to call `Operator` in async runtime and how to use OpenDAL to read and write data!

## Warmup

Before delving into the OpenDAL API, let's take a look at some new Rust features.

### Crate Features

In this chapter's `Cargo.toml`, we add a new dependence `tokio`:

```toml
tokio = { version = "1", features = ["full"] }
```

The syntax is different from what we used before:

```diff
- tokio = "1"
+ tokio = { version = "1", features = ["full"] }
```

We use this syntax to specify additional options for this dependency. Essentially, `tokio = "1"` can be considered a shorthand for `tokio = { version = "1" }`.

The most commonly used option is `features`, which works in conjunction with conditional compilation and the crate's configuration system. These features allow you to enable or disable specific pieces of code within the crate based on different use cases.

For instance, since the `tokio` project has many components, not all of them are necessary for every project. By enabling only the required features, we can reduce build time.

In our examples, we will just enable the `macros` and `"rt-multi-thread` feature which allow use to use `#[tokio::main]` in code.

### Attributes

Let's back to read our `main.rs`. We will find a new thing here: `#[tokio::main]`: the attribute macros denoted by the `#[...]` syntax and are used to add additional metadata or behavior to the annotated item.

`[tokio::main]` here will expand the following code:

```rust
#[tokio::main]
async fn main() {
    println!("Hello world");
}
```

into:

```rust
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            println!("Hello world");
        })
}
```

There are many different kinds of attributes in rust, we can refer to [The Rust Reference - Attributes](https://doc.rust-lang.org/reference/attributes.html#attributes) for more information.

## Async Function

Another new thing here is `async fn`. By prefix `async` before `fn`, we can declare an async function:

```rust
async fn write_data(op: Operator) -> Result<()> {
    ...
}
```

Every async function in Rust is essentially a state machine generator. When we call an async function, it creates a new state machine that can be polled to determine if it has finished running or not during runtime. We described this state machine as a [`Future`](https://doc.rust-lang.org/std/future/trait.Future.html).

If you call an async function without using `await`, it will not do anything because we only create a `Future` but do not poll it. We will need to call async function like the following:

```rust
async fn write_data(op: Operator) -> Result<()> {
    op.write("test", "Hello, World!").await?;

    Ok(())
}
```

When using `.await`, we submit the `Future` created by `op.write("test", "Hello, World!")` to the async runtime and continuously poll it until it finishes.

The async runtime will have multiple workers running different `Future` tasks, allowing us to avoid blocking on IO and improving the performance of the entire application.

## Write Data

Now we can go deeper into OpenDAL's API. Writing data via OpenDAL is simple:

```rust
async fn write_data(op: Operator) -> Result<()> {
    op.write("test", "Hello, World!").await?;

    Ok(())
}
```

The `write` API provided by `Operator` is:

```rust
impl Operator {
  pub async fn write(&self, path: &str, bs: impl Into<Bytes>) -> Result<()>;
}
```

`impl Into<Bytes>` here is a syntax sugar of rust, we can expand it like the following:

```rust
impl Operator {
  pub async fn write<T>(&self, path: &str, bs: T) -> Result<()>
    where T: Into<Bytes>;
}
```

This API means it accepts any types that implements `Into<Bytes>`. `Bytes` is provided by [`bytes::Bytes`](https://docs.rs/bytes/latest/bytes/struct.Bytes.html), we can find all types that implements `Into<Bytes>` here.

So we can also calling `write` on:

- `Vec<u8>`
- `String`
- `&'static str`

For example:

```rust
async fn write_data(op: Operator) -> Result<()> {
    let s = "Hello, World!".to_string();
    op.write("test", s).await?;

    Ok(())
}
```

The easiest way to write data into OpenDAL is by calling the "write" function. However, OpenDAL also offers additional extensions for adding metadata to files and writing data in a streaming manner, which eliminates the need to hold all of the data in memory. We will cover these topics in upcoming chapters.

## Read Data

Reading data via OpenDAL is simple too:

```rust
async fn read_data(op: Operator) -> Result<()> {
    let bs = op.read("test").await?;
    println!("read data: {}", String::from_utf8_lossy(&bs));

    Ok(())
}
```

The `read` API provided by `Operator` is:

```rust
pub async fn read(&self, path: &str) -> Result<Vec<u8>>;
```

This API will read all data from `path` and return as a `Vec<u8>`.

## Conclusion

In this chapter we learnt a lot basic concepts in async rust! Now we have known that:

- How to setup tokio async runtime.
- How to define and call an async function.
- How to write and read data via OpenDAL.

## Challenge Time

Now that we have mastered all the knowledge required to write and read data via OpenDAL. Let's take on a challenge!

Our challenge is to

- Write and read data from AWS S3.
- Explore the API related to `read_with` and `write_with`.

See you in the next chapter!
