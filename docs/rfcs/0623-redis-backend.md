- Proposal Name: "redis_backend"
- Start Date: 2022-08-31
- RFC PR: [datafuselabs/opendal#0623](https://github.com/datafuselabs/opendal/pull/0623)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Use [redis](https://redis.io) as a backend of OpenDAL.

# Motivation

Redis is a fast, in-memory cache with persistent and distributed storage functionalities. It's widely used in production.

Users also demand more backend support. Redis is a good candidate.

# Guide-level explanation

Users only need to provide the network address, username and password to create a Redis Operator. Then everything will act as other operators do.

```rust
use opendal::services::redis::Builder;
use opendal::Operator;

let builder = Builder::default();

// set the endpoint of redis server
builder.endpoint("redis://127.0.0.1:2333");
// set the username of redis
builder.username("example");
// set the password
builder.password(&std::env::var("OPENDAL_REDIS_PASSWORD").expect("env OPENDAL_REDIS_PASSWORD not set"));
// root path
builder.root("/example/");

let rds: opendal::services::redis::RedisAccessor = builder.build().unwrap();
let op = Operator::new(rds);

// congratulations, you can use `op` just like any other operators!
```

# Reference-level explanation

To ease the development, [redis-rs](https://crates.io/crates/redis) will be used.

Redis offers a key-value view, so the path of files could be represented as the key of the key-value pair.

Files in opendal will be represented as `Hash` in Redis, a kind of semi-structured data type:

```
                                           ┌───────────────────────────┐
                                           │  "mode" : "file"          │
Key: /home/monika/                         ├───────────────────────────┤
                                           │  "last_modified" :        │
Key: /home/monika/poem0.txt ─────────────► │  "<rfc3339 timestamp>"    │
                                           ├───────────────────────────┤
Key: /home/monika/characters/              │  "content_length" :       │
                                           │  "224"                    │
Key: /home/monika/characters/you.chr       ├───────────────────────────┤
                                           │  "content_md5" :          │
                                           │  "<md5_string>"           │
                                           ├───────────────────────────┤
                                           │  other metadata fields    │
                                           ├───────────────────────────┤
                                           │  "content" :              │
                                           │  "<real content of file>" │
                                           └───────────────────────────┘
```

The [redis-rs](https://crates.io/crates/redis)'s high level APIs is preferred.

```rust
let client = redis::Client::open("redis://localhost:6379")?;
let con = client.get_async_connection()?;
```

## Create File

If user is creating a file with root `/home/monika/`, and relative path `poem0.txt`, the `HSET` command of Redis will be called.

```rust
// mode: ObjectMode
// path: relative path string
let path = get_abs_path(path);  // /home/monika/ <> /poem.txt -> /home/monika/poem.txt
let last_modified = OffsetDatetime::now_utc().to_string();

let kvs = [("content", String::new()), ("last_modified", last_modified), ("content_length", "0".to_string()), ("content_md5", "".to_string())];
con.hset_multiple(path, &kvs)
    .await
    .map_err(|e| other(BackendError::new(..)))?;
```

```redis
HSET /home/monika/poem0.txt mode file
```

This will create a key-value pair, whose key is `/home/monika/poem0.txt`, value is a string to string hashmap containing a tuple `("mode", "file")`.

## Read File

Opendal empowers users to read with the `path` object, `offset` of the cursor and `size` to read. Sadly in redis each time reading the whole object's data will be transmitted, making it opendal's responsibility to move the cursor and to discard the bytes read.

For example, user want to read from poem0.txt. The content of file will be stored in `content` field.

```rust
// path: "poem0.txt"
// offset: Option<u64>, the offset of reading
// size: Option<u64>, the size of reading
let path = get_abs_path(path);
let buf: Vec<u8> = con.hget(path, "content")
    .await
    .map_err(|e| {
        other(e)
    });
let mut cursor = futures::io::Cursor::new(buf);
if let Some(offset) = offset {
    cursor.seek(SeekFrom::Start(offset)).await;
}
match size {
    Some(size) => {
        Box::new(cursor.take(size))
    }
    None => {
        Box::new(cursor)
    }
}
```

```redis
HGET /home/monika/poem0.txt content
```

## Write File

Redis ensures the writing of a single entry to be atomic, no locking is required.

What needs to take care by opendal, besides the content of object, is its metadata. For example, though offering a `OBJECT IDLETIME` command, redis cannot record the last modified time of a key, so this should be done in opendal.

```rust
use redis::AsyncCommands;
// args: &OpWrite
// r: BytesReader
let mut buf = vec![];
let content_length: u64 = futures::io::copy(r, &mut buf).await?;
let content_md5: String = self.md5(buf);
let last_modified: String = OffsetDateTime::now().to_string();

let path = get_abs_path(args.path());
let kvs = [("content_length", content_length.to_string()), ("last_modified", last_modified), ("content", content), ("content_md5", content_md5)];

con.hset_multiple(path, &kvs).await;
```

```redis
HSET /home/monika/poem.txt content_length 5 last_modified rfc3339_timestamp content content_string content_md5 md5_string
```

## Stat

To get the metadata of an object, using the `HMGET` command.

```rust
let path = get_abs_path(args.path());
let (mode, content_length, last_modified, content_md5): (String, u64, String, String) =
        redis::cmd("HMGET").arg(path).arg(&["mode", "content_length", "last_modified", "content_md5"]).query_async(&mut con).await?;
```

```redis
HMGET /home/monika/poem.txt mode content_length last_modified content_md5
```

## Delete

All subdirectories of path will be listed and removed. To list all subdirectories and the file or directory itself, call the `KEYS` command of redis.

```rust
let path = get_abs_path(args.path());

let collect = if !path.ends_with('/') {
    let s: Vec<String> = con.keys(&path).await?;
    let sub: Vec<String> = con.keys(path + "/*").await?;
    s.into_iter().chain(sub.into_iter()).collect::<Vec<String>>()
} else {
    let s: Vec<String> = con.keys(&path).await?;
    let sub: Vec<String> = con.keys(path + "*").await?;
    s.into_iter().chain(sub.into_iter()).collect::<Vec<String>>()
};

for de in path.iter() {
    let is_ok: bool = con.del(de).await?;
    assert!(is_ok);
}
```

```redis
KEYS /home/monika
KEYS /home/monika/*
DEL /home/monika/poem.txt
```

## List

Redis only supports glob patterns, some filtering have to be done on the opendal side.

This could be done by listing keys matching glob patterns first, then filter out keys not in current level of directory, wrap their metadata into `DirEntry`s and return them to upper callers.

## Blocking APIs

`redis-rs` also offers a synchronous version of API, just port the functions above to its synchronous version.

# Drawbacks

1. A new dependency is introduced;
2. The redis documentation unrecommended the using of `KEYS`;
3. Some calculations have to be done in client side, this will affect the performance;
4. Grouping atomic operations together doesn't promise transactional access, this may lead to data racing issues.

# Rationale and alternatives

The [`RedisJSON`](https://redis.io/docs/stack/json/) module provides JSON support for Redis, and supports depth up to 128. Working on a JSON api could be easier than operating on 2 levels of mapping.

This requires users to load a module on redis.

# Prior art

None

# Unresolved questions

None

# Future possibilities

The implementation proposed here is far from perfect. 

1. The data organization could be optimized to make it acts more like a filesystem;
2. Switching to RedisJSON API;
3. Making a customized redis module to calculate metadata on redis side..