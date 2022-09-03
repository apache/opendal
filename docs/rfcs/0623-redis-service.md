- Proposal Name: `redis_service`
- Start Date: 2022-08-31
- RFC PR: [datafuselabs/opendal#0623](https://github.com/datafuselabs/opendal/pull/0623)
- Tracking Issue: [datafuselabs/opendal#0000](https://github.com/datafuselabs/opendal/issues/0000)

# Summary

Use [redis](https://redis.io) as a service of OpenDAL.

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
builder.endpoint("tcps://domain.to.redis:2333");
// set the username of redis
builder.username("example");
// set the password
builder.password(&std::env::var("OPENDAL_REDIS_PASSWORD").expect("env OPENDAL_REDIS_PASSWORD not set"));
// root path
builder.root("/example/");

let backend = builder.build().unwrap(); // opendal::services::redis::Backend
let op = Operator::new(backend);

// congratulations, you can use `op` just like any other operators!
```

# Reference-level explanation

To ease the development, [redis-rs](https://crates.io/crates/redis) will be used.

Redis offers a key-value view, so the path of files could be represented as the key of the key-value pair.

The content of file will be represented directly as `String`, and the metadata of objects will be represented as `Hash` in Redis, a kind of semi-structured data type:

```
Object: /home/monika/poem0.txt

content:  Key: /c/home/monika/poem0.txt────┐
                                           │
metadata: Key: /m/home/monika/poem0.txt    │
│                                          ▼
│   HASH                                  STRING
│  ┌───────────────────────────┐         ┌────────────────────┐
└► │  "mode" : "file"          │         │1JU5T3MON1K413097321│
   ├───────────────────────────┤         │&JU5$T!M0N1K4$%#@#$%│
   │  "last_modified" :        │         │3231J)U_ST#MONIKA@#$│
   │  "<rfc3339 timestamp>"    │         │1557(m0N1ka3just4M  │
   ├───────────────────────────┤         │      ...           │
   │  "content_length" :       │         └────────────────────┘
   │  "224"                    │
   ├───────────────────────────┤
   │  "content_md5" :          │
   │  "<md5_string>"           │
   ├───────────────────────────┤
   │  other metadata fields    │
   └───────────────────────────┘
```

The [redis-rs](https://crates.io/crates/redis)'s high level APIs is preferred.

```rust
let client = redis::Client::open("redis://localhost:6379")?;
let con = client.get_async_connection()?;
```

## Create File

If user is creating a file with root `/home/monika/`, and relative path `poem0.txt`.

```rust
// mode: ObjectMode
// path: relative path string
let path = get_abs_path(path);  // /home/monika/ <> /poem.txt -> /home/monika/poem.txt
let m_path = "/m".to_string() + &path;  // path of metadata
let c_path = "/c".to_string() + &path;  // path of content
let last_modified = OffsetDatetime::now_utc().to_string();

let kvs = [("last_modified", last_modified), ("content_length", "0".to_string()), ("content_md5", "".to_string())];
con.hset_multiple(m_path, &kvs)
    .await
    .map_err(|e| other(BackendError::new(..)))?;
con.set(c_path, "".to_string())?;
```

This will create two key-value pair. For object content, its key is `/c/home/monika/poem0.txt`, the value is an empty `String`; For metadata, the key is `/home/monika/poem0.txt`, the value is a string to string hashmap containing tuples like `("mode", "file")`, `("last_modified", <rfc3339 timestamp string>)`.

## Read File

Opendal empowers users to read with the `path` object, `offset` of the cursor and `size` to read. Redis provided a `GETRANGE` command which perfectly fit into it.

```rust
// path: "poem0.txt"
// offset: Option<u64>, the offset of reading
// size: Option<u64>, the size of reading
let path = get_abs_path(path);
let c_path = "/c".to_string() + &path;
let (mut start, mut end) = (0, -1);
if let Some(offset) = offset {
    start = offset;
}
if let Some(size) = size {
    end = start + size;
}
let buf: Vec<u8> = con.getrange(c_path, start, end).await?;
Box::new(buf)
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
let m_path = "/m".to_string() + &path;
let c_path = "/c".to_string() + &path;
let kvs = [("content_length", content_length.to_string()), ("last_modified", last_modified), ("content_md5", content_md5)];

con.set(c_path, content).await?;
con.hset_multiple(m_path, &kvs).await?;
```

```redis
SET /c/home/monika/poem.txt content_string
HSET /m/home/monika/poem.txt content_length 5 last_modified rfc3339_timestamp content_md5 md5_string
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

## List

Redis only supports glob patterns, some filtering have to be done on the opendal side.

This could be done by listing keys matching glob patterns first, with `SCAN` and `MATCH`, then filter out keys not in current level of directory, wrap their metadata into `DirEntry`s and return them to upper callers.

## Delete

All subdirectories of path will be listed and removed. To list all subdirectories and the file or directory itself.

This could be done similarly to `List`, iterating on the pattern with `SCAN` and `MATCH`, and then remove all files and directories matched.

## Blocking APIs

`redis-rs` also offers a synchronous version of API, just port the functions above to its synchronous version.

# Drawbacks

1. A new dependency is introduced;
2. Some calculations have to be done in client side, this will affect the performance;
3. Grouping atomic operations together doesn't promise transactional access, this may lead to data racing issues.
4. Writing large binary strings requiring copying all data from pipe(or `BytesReader` in opendal) to RAM, and then send to redis.

# Rationale and alternatives

## Store Metadata with `bincode`

Instead of storing with hashmap, use bincode to store metadata.

## RedisJSON module
The [`RedisJSON`](https://redis.io/docs/stack/json/) module provides JSON support for Redis, and supports depth up to 128. Working on a JSON api could be easier than operating on 2 levels of mapping.

This requires users to load a module on redis.

# Prior art

None

# Unresolved questions

None

# Future possibilities

The implementation proposed here is far from perfect. 

- The data organization could be optimized to make it acts more like a filesystem
- Switching to RedisJSON API
- Making a customized redis module to calculate metadata on redis side
