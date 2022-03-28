- Proposal Name: `async_streaming_io`
- Start Date: 2022-03-28
- RFC PR: [datafuselabs/opendal#191](https://github.com/datafuselabs/opendal/pull/191)
- Tracking Issue: [datafuselabs/opendal#190](https://github.com/datafuselabs/opendal/issues/190)

# Summary

Use `Stream`/`Sink` instead of `AsyncRead` in `Accessor`.

# Motivation

`Accessor` intends to be the `underlying trait of all backends for implementors`. However, it's not so underlying enough.

## Over-wrapped

`Accessor` returns a `BoxedAsyncReader` for `read` operation:

```rust
pub type BoxedAsyncReader = Box<dyn AsyncRead + Unpin + Send>;

pub trait Accessor {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let _ = args;
        unimplemented!()
    }
}
```

And we are exposing `Reader`, which implements `AsyncRead` and `AsyncSeek` to end-users. For every call to `Reader::poll_read()`, we need:

- `Reader::poll_read()`
- `BoxedAsyncReader::poll_read()`
- `IntoAsyncRead<ByteStream>::poll_read()`
- `ByteStream::poll_next()`

If we could return a `Stream` directly, we can transform the call stack into:

- `Reader::poll_read()`
- `ByteStream::poll_next()`

In this way, we operate on the underlying IO stream, and the caller must keep track of the reading states.

## Inconsistent

OpenDAL's `read` and `write` behavior is not consistent.

```rust
pub type BoxedAsyncReader = Box<dyn AsyncRead + Unpin + Send>;

pub trait Accessor: Send + Sync + Debug {
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let _ = args;
        unimplemented!()
    }
    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> Result<usize> {
        let (_, _) = (r, args);
        unimplemented!()
    }
}
```

For `read`, OpenDAL returns a `BoxedAsyncReader` which users can decide when and how to read data. But for `write`, OpenDAL accepts a `BoxedAsyncReader` instead, in which users can't control the writing logic. How large will the writing buffer size be? When to call `flush`?

## Service native optimization

OpenDAL knows more about the service detail, but returning `BoxedAsyncReader` makes it can't fully use the advantage.

For example, most object storage services use HTTP to transfer data which is TCP stream-based. The most efficient way is to return a full TCP buffer, but users don't know about that. First, users could have continuous small reads on stream. To overcome the poor performance, they have to use `BufReader`, which adds a new buffering between reading. Then, users don't know the correct (best) buffer size to set.

Via returning a `Stream`, users could benefit from it in both ways:

- Users who want underlying control can operate on the `Stream` directly.
- Users who don't care about the behavior can use OpenDAL provided Reader, which always adopts the best optimization.

# Guide-level explanation

Within the `async_streaming_io` feature, we will add the following new APIs to `Object`:

```rust
impl Object {
    pub async fn stream(&self, offset: Option<u64>, size: Option<u64>) -> Result<BytesStream> {}
    pub async fn sink(&self, size: u64) -> Result<BytesSink> {}
}
```

Users can control the underlying logic of those bytes, streams, and sinks.

For example, they can:

- Read data on demand: `stream.next().await`
- Write data on demand: `sink.feed(bs).await; sink.close().await;`

Based on `stream` and `sink`, `Object` will provide more optimized helper functions like:

- `async read(offset: Option<u64>, size: Option<u64>) -> Result<bytes::Bytes>`
- `async write(bs: bytes::Bytes) -> Result<()>`

# Reference-level explanation

`read` and `write` in `Accessor` will be refactored into streaming-based:

```rust
pub type BytesStream =  Box<dyn Stream + Unpin + Send>;
pub type BytesSink =  Box<dyn Sink + Unpin + Send>;

pub trait Accessor: Send + Sync + Debug {
    async fn read(&self, args: &OpRead) -> Result<BytesStream> {
        let _ = args;
        unimplemented!()
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesSink> {
        let _ = args;
        unimplemented!()
    }
}
```

All other IO functions will be adapted to fit these changes.

For fs, it's simple to implement `Stream` and `Sink` for `tokio::fs::File`.

We will return a `BodySinker` instead for all HTTP-based storage services. In which we maintain a `put_object` `ResponseFuture` that construct by `hyper` and a `sender` part of the channel. All data sent by users will be passed to `ResponseFuture` via the unbuffered channel.

```rust
struct BodySinker {
    fut: ResponseFuture,
    sender: Sender<bytes::Bytes>
}
```

# Drawbacks

None.

# Rationale and alternatives

## Performance for the extra channel in `write`

Based on the benchmark during research, the **unbuffered** channel does improve the performance a bit in some cases:

Before:

```rust
write_once/4.00 KiB     time:   [564.11 us 575.17 us 586.15 us]
                        thrpt:  [6.6642 MiB/s 6.7914 MiB/s 6.9246 MiB/s]
write_once/256 KiB      time:   [1.3600 ms 1.3896 ms 1.4168 ms]
                        thrpt:  [176.46 MiB/s 179.90 MiB/s 183.82 MiB/s]
write_once/4.00 MiB     time:   [11.394 ms 11.555 ms 11.717 ms]
                        thrpt:  [341.39 MiB/s 346.18 MiB/s 351.07 MiB/s]
write_once/16.0 MiB     time:   [41.829 ms 42.645 ms 43.454 ms]
                        thrpt:  [368.20 MiB/s 375.19 MiB/s 382.51 MiB/s]
```

After:

```rust
write_once/4.00 KiB     time:   [572.20 us 583.62 us 595.21 us]
                        thrpt:  [6.5628 MiB/s 6.6932 MiB/s 6.8267 MiB/s]
                 change:
                        time:   [-6.3126% -3.8179% -1.0733%] (p = 0.00 < 0.05)
                        thrpt:  [+1.0849% +3.9695% +6.7380%]
                        Performance has improved.
write_once/256 KiB      time:   [1.3192 ms 1.3456 ms 1.3738 ms]
                        thrpt:  [181.98 MiB/s 185.79 MiB/s 189.50 MiB/s]
                 change:
                        time:   [-0.5899% +1.7476% +4.1037%] (p = 0.15 > 0.05)
                        thrpt:  [-3.9420% -1.7176% +0.5934%]
                        No change in performance detected.
write_once/4.00 MiB     time:   [10.855 ms 11.039 ms 11.228 ms]
                        thrpt:  [356.25 MiB/s 362.34 MiB/s 368.51 MiB/s]
                 change:
                        time:   [-6.9651% -4.8176% -2.5681%] (p = 0.00 < 0.05)
                        thrpt:  [+2.6358% +5.0614% +7.4866%]
                        Performance has improved.
write_once/16.0 MiB     time:   [38.706 ms 39.577 ms 40.457 ms]
                        thrpt:  [395.48 MiB/s 404.27 MiB/s 413.37 MiB/s]
                 change:
                        time:   [-10.829% -8.3611% -5.8702%] (p = 0.00 < 0.05)
                        thrpt:  [+6.2363% +9.1240% +12.145%]
                        Performance has improved.
```

## Add complexity on the services side

Returning `Stream` and `Sink` make it complex to implement. At first glance, it does. But in reality, it's not.

Note: HTTP (especially for hyper) is stream-oriented.

- Returning a `stream` is more straightforward than `reader`.
- Returning `Sink` is covered by the global shared `BodySinker` struct.

Other helper functions will be covered at the Object-level which services don't need to bother.

# Prior art

## Returning a `Writer`

The most natural extending is to return `BoxedAsyncWriter`:

```rust
pub trait Accessor: Send + Sync + Debug {
    /// Read data from the underlying storage into input writer.
    async fn read(&self, args: &OpRead) -> Result<BoxedAsyncReader> {
        let _ = args;
        unimplemented!()
    }
    /// Write data from input reader to the underlying storage.
    async fn write(&self, args: &OpWrite) -> Result<BoxedAsyncWriter> {
        let _ = args;
        unimplemented!()
    }
}
```

But it only fixes the `Inconsistent` concern and can't help with other issues.

## Slice based API

Most rust IO APIs are based on slice:

```rust
pub trait Accessor: Send + Sync + Debug {
    /// Read data from the underlying storage into input writer.
    async fn read(&self, args: &OpRead, bs: &mut [u8]) -> Result<usize> {
        let _ = args;
        unimplemented!()
    }
    /// Write data from input reader to the underlying storage.
    async fn write(&self, args: &OpWrite, bs: &[u8]) -> Result<usize> {
        let _ = args;
        unimplemented!()
    }
}
```

The problem is `Accessor` doesn't have states:

- If we require all data must be passed at one time, we can't support large files read & write
- If we allow users to call `read`/`write` multiple times, we need to implement another `Reader` and `Writer` alike logic.

## Accept `Reader` and `Writer`

It's also possible to accept `Reader` and `Writer` instead.

```rust
pub trait Accessor: Send + Sync + Debug {
    /// Read data from the underlying storage into input writer.
    async fn read(&self, args: &OpRead, w: BoxedAsyncWriter) -> Result<usize> {
        let _ = args;
        unimplemented!()
    }
    /// Write data from input reader to the underlying storage.
    async fn write(&self, args: &OpWrite, r: BoxedAsyncReader) -> Result<usize> {
        let _ = args;
        unimplemented!()
    }
}
```

This API design addressed all concerns but made it hard for users to use. Primarily, we can't support `futures::AsyncRead` and `tokio::AsyncRead` simultaneously.

# Unresolved questions

None.

# Future possibilities

- Implement `Object::read_into(w: BoxedAsyncWriter)`
- Implement `Object::write_from(r: BoxedAsyncReader)`
