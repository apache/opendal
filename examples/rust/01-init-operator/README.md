# Chapter-01: Initiate OpenDAL Operator

The core concept in OpenDAL is the `Operator`, which is used to handle operations on different storage services. This chapter will discuss how to initiate OpenDAL operators.

## Warmup

Before delving into the OpenDAL API, let's take a look at some new Rust features.

### Enum and Set Returning Type for Function

First of all, our main functions now returns `Result<()>`:

```rust
use opendal::Result;

fn main() -> Result<()> {
    Ok(())
}
```

This is one of the coolest part of Rust: [`enum`](https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html). In rust, an `enum` (short for enumeration) is a data type that represents a value that can be one of several possible variants. It allows you to define a set of distinct options or choices, making it useful for scenarios where a variable can have a limited number of predefined states.

For example, Rust provide a built-in enum called `Option`:

```rust
enum Option<T> {
    Some(T),
    None,
}
```

The `T` represents the type of the value that may or may not be present. `Some` variant holds the actual value, while `None` indicates the absence of a value.

Rust also provides a `Result` enum for returning and propagating errors:

```rust
enum Result<T, E> {
   Ok(T),
   Err(E),
}
```

OpenDAL defines it own `Result` type for it easier to use:

```rust
/// Result that is a wrapper of `Result<T, opendal::Error>`
pub type Result<T> = std::result::Result<T, Error>;
```

So let's go back into our examples:

```rust
use opendal::Result;

fn main() -> Result<()> {
    Ok(())
}
```

> `use opendal::Result;`

imports opendal's `Result` which will shadows rust built-in result.

> `fn main() -> Result<()>`

The way that rust to declare returning types of a function. This case means `main` will return a `Result` which holds `()`.

> `()`

The `()` type has exactly one value `()`, and is used when there is no other meaningful value that could be returned. In fact, the example in `00-setup` could is exactly `fn main() -> () {}`.

> `Ok(())`

The variants of `Result` are preluded, allowing us to use `Ok` and `Err` directly instead of having to write out `Result::Ok` and `Result::Err`. In this context, using `Ok(())` means creating a new value for the type `Result<()>`.

### Call Functions

Let's go next line:

```rust
let op = init_operator_via_builder()?;
```

- `let a = b;` is the syntax in Rust for declaring an immutable variable.
- `xxx()` means call function that named `xxx`.
- the `?` is a syntax sugar in Rust that checks if the returned `Result` is an `Err`. If it is, then it immediately returns that error. Otherwise, it returns the value of `Ok`.

So this line will call `init_operator_via_builder` function first and set to variable `op` if it's returns `Ok`.

## Initiate Via Builder

Now, we can go deeper into the OpenDAL side.

Let's take look over `init_operator_via_builder` function first.

```rust
fn init_operator_via_builder() -> Result<Operator> {
    let mut builder = S3::default();
    builder.bucket("example");
    builder.access_key_id("access_key_id");
    builder.secret_access_key("secret_access_key");
    builder.region("us-east-1");

    let op = Operator::new(builder)?.finish();
    Ok(op)
}
```

We have a new concept here:

> `let mut builder = xxx;

The `mut` here means `mutable`, allowing its value to be changed later.

In Rust, all variables are declared as `immutable` by default, meaning that users cannot change their values. However, by declaring them as `mut`, we can call mutable functions such as `bucket` and `access_key_id`.

In this example, we initiate a new builder and call its functions to set different arguments. Finally, we use `Operator::new(builder)?.finish()` to build it.

Each service provides its own builder, which can be accessed at [opendal::services](https://opendal.apache.org/docs/rust/opendal/services/index.html).

## Initiate Via Map

Calling builder's API is simple and directly, but sometimes developer will read config from files or env which will need to building operator from a map.

```rust
fn init_operator_via_map() -> Result<Operator> {
    let mut map = HashMap::default();
    map.insert("bucket".to_string(), "example".to_string());
    map.insert("access_key_id".to_string(), "access_key_id".to_string());
    map.insert(
        "secret_access_key".to_string(),
        "secret_access_key".to_string(),
    );
    map.insert("region".to_string(), "us-east-1".to_string());

    let op = Operator::via_map(Scheme::S3, map)?;
    Ok(op)
}
```

In this example, we use `Operator::via_map()` API the init a new operator.

## Let's Go!

We will see the output of this example in this way:

```shell
:) cargo run
   Compiling init-operator v0.1.0 (/home/xuanwo/Code/apache/opendal/examples/rust/01-init-operator)
    Finished dev [unoptimized + debuginfo] target(s) in 0.38s
     Running `target/debug/init-operator`
operator from builder: Operator { accessor: S3Backend { core: S3Core { bucket: "example", endpoint: "https://s3.us-east-1.amazonaws.com/example", root: "/", .. } }, limit: 1000 }
operator from map: Operator { accessor: S3Backend { core: S3Core { bucket: "example", endpoint: "https://s3.us-east-1.amazonaws.com/example", root: "/", .. } }, limit: 1000 }
```

Please feel free to try initiating other services or configuring new settings for experimentation purposes!

## Conclusion

In this chapter we learnt a lot basic concepts in rust! Now we have known that:

- How to define a function that returns something.
- How to declare an immutable variable.
- How to call functions in Rust.
- How to define and use enums.
- How to handling result.

With our newly acquired rust skills, we can initiate the OpenDAL Operator in two main ways. This knowledge will be used to read and write data to storage in the next chapter!
