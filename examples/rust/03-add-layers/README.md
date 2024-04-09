# Chapter-03: How To Add Layers

In the preceding chapters, we have effectively utilized OpenDAL to access the storage service and execute basic data read and write operations. This chapter takes a step forward by introducing additional layers to manage data operations, allowing for controlled access behavior and the acquisition of observable data throughout the process.

## What are Layers in OpenDAL

OpenDAL offers native layer support, enabling users to implement middleware or intercept for all operations. By using layers, we can retry failed requests and resume from the point of failure with Retry Layer, provide native observability with Tracing Layer, and so on.

## Simple Examples

In this section, we will incorporate practical Layers into our OpenDAL data reading and writing operations, allowing us to experience the simplicity and convenience they offer. You can find the complete code [here](./src/main.rs).

### Adding Logging Capability

Logging is an invaluable feature in daily development. OpenDAL offers comprehensive logging support throughout various data access operations. Specifically, OpenDAL utilizes [log](https://docs.rs/log/latest/log/) crate to record structured logs. At the onset of each operation, an initial log entry is made. Upon completion of each operation, a log entry is recorded to indicate its success or failure along with the specific reasons for any failures encountered.

```diff
+ env_logger = "0.11"
```

To utilize logging functionality, we need introduce the [env_logger](https://docs.rs/env_logger/latest/env_logger/) crate first. This is necessary because the [log](https://docs.rs/log/latest/log/) crate in Rust solely offers a unified API abstraction for logging. Users can select the appropriate crate and specific logging implementation based on their actual requirements. Here, we opt for the [env_logger](https://docs.rs/env_logger/latest/env_logger/) crate.

```rust
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let op = Operator::new(Memory::default())
        .expect("must init")
        .layer(LoggingLayer::default())
        .finish();
    ...
    Ok(())
}
```

We simply need to add a layer to the creation process of the `Operator` in a chain call to apply the functions of the corresponding layers to the `Operator`. Here, we add a default Logger Layer for the `Operator`. Through log outputs, we can gain a clearer understanding of the operations performed by OpenDAL throughout the entire process, as well as the execution status of each operation.

```shell
$ RUST_LOG=debug cargo run
[DEBUG opendal::services] service=memory operation=metadata -> started
[DEBUG opendal::services] service=memory operation=metadata -> finished: AccessorInfo { scheme: Memory, root: "/", name: "0x5565075f1ad0", native_capability: { Stat | Read | Write | Delete | List | Blocking }, full_capability: { Stat | Read | Write | CreateDir | Delete | List | Blocking } }
[DEBUG opendal::services] service=memory operation=write path=test -> started
[DEBUG opendal::services] service=memory operation=write path=test -> start writing
[DEBUG opendal::services] service=memory operation=write path=test written=13B -> data written finished
[DEBUG opendal::services] service=memory operation=stat path=test -> started
[DEBUG opendal::services] service=memory operation=stat path=test -> finished: RpStat { meta: Metadata { metakey: FlagSet(Complete | Mode | ContentLength), mode: FILE, cache_control: None, content_disposition: None, content_length: Some(13), content_md5: None, content_range: None, content_type: None, etag: None, last_modified: None, version: None } }
[DEBUG opendal::services] service=memory operation=read path=test range=0-12 -> started
[DEBUG opendal::services] service=memory operation=read path=test range=0-12 -> got reader
[DEBUG opendal::services] service=memory operation=read path=test read=13 -> data read finished
data: Hello, World!
```

Next let's execute the code. It's important to note that when running the code, you'll need to include RUST_LOG and specify the log output level. After running the sample code, you'll see output similar to the above on your terminal. By enabling Logging Layer, OpenDAL no longer appears as a black box! By controlling the location and level of log output, you can access all the details you wish to know.

### Stacking Timeout Layer

OpenDAL not only supports adding a layer to the `Operator`, but also enables the addition of all necessary layers to the `Operator` through chain calls. Let's continue by stacking the Timeout Layer on top of the Logger Layer.

```Rust
let op = Operator::new(Memory::default())
    .expect("must init")
    .layer(LoggingLayer::default())
    .layer(
        TimeoutLayer::default()
            .with_timeout(Duration::from_secs(5))
            .with_io_timeout(Duration::from_secs(3))
    )
    .finish();
```

We can directly add the Timeout Layer in a unified manner based on the previous code. Each operation in the `Operator` can be categorized into IO operations and non-IO operations. Users can set different timeouts for each operation according to their actual needs. In this example, we set a 5-second timeout for non-IO operations and a 3-second timeout for IO operations in the `Operator`.

Through the Timeout Layer, we can prevent any operation in OpenDAL from waiting excessively long or getting stuck indefinitely due to unforeseen circumstances. For instance, a dead connection might cause a SQL query to hang indefinitely. The Timeout Layer will terminate such connections and return an error, enabling users to handle them by retrying or directly informing the users.

## More Useful Layers

### Retry Layer

The Retry Layer provides the ability for OpenDAL to automatically retry related operations when an error occurs in a data access operation based on OpenDAL.

OpenDAL uses an exponential backoff algorithm to retry erroneous requests to avoid request current limiting and conflicts to the greatest extent, and provides relevant settings to further control retry behavior. Specifically, OpenDAL supports setting the maximum number of retries, the minimum retry interval, the maximum retry interval, the jitter and exponential coefficients in the exponential backoff algorithm, and setting the callback function for each retry.

In addition, OpenDAL divides errors in requests into three types: `Permanent`, `Tempory` and `Persistent`:

- `Permanent` means that the error cannot be solved by retrying, such as `Not Found` error and permission authentication error. At this time, OpenDAL will not retry the error request to avoid adding additional unnecessary request traffic;
- `Temporary` means that the error is temporary and may be resolved by retrying. For example, the storage service returns a traffic restriction error. After adding the Retry Layer, OpenDAL will automatically retry this type of request;
- `Persistent` means that the request error cannot be resolved after retrying, such as continuing network error. `Persistent` is transformed from `Temporary`.

Users can further judge the service running status based on the error type returned by the OpenDAL request.

### Concurrent Limit Layer

Through the Concurrency Limit Layer, users can control the maximum number of concurrent connections between OpenDAL and the underlying storage service. This control can effectively manage system resources and avoid the risk of excessive resource usage leading to performance degradation or system crash.

### Blocking Layer

As introduced in the previous chapters, OpenDAL uses the Rust asynchronous runtime to ensure efficient processing of IO requests. Blocking Layer provides a synchronous semantic wrapper for operations in OpenDAL, allowing users to bridge the use of many underlying asynchronous services provided by OpenDAL with synchronous semantics.

In addition, since synchronization bridging involving asynchronous functions involves knowledge about the [Tokio](https://docs.rs/tokio/latest/tokio/) runtime, users need additional processing when using this type of interface. This is beyond the scope of this tutorial, more examples can be found in https://docs.rs/opendal/latest/opendal/layers/struct.BlockingLayer.html.

### Built-in Layers in OpenDAL

An interesting fact is that OpenDAL applies some layers by default for every operation, such as Error Context Layer and Complete Layer. These built-in layers are usually very thin, light and in a zero-cost way. So there is no need to worry about its impact on data access performance, but they can greatly enhance the user experience.

Error Context Layer injects context information, such as service (the `Scheme` of underlying service), operation (the `Operation` of this operation), path (the path of this operation), into all returned errors of operations.

Complete Layer adds necessary capabilities to services, complete underlying services features. OpenDAL exposes a unified interface implementation through Complete Layer, so that users can use them in the same way, and OpenDAL will always choose the optimal way to initiate the request.

For example, S3 cannot support `seek`, but OpenDAL will implement it at zero cost manner according to existing methods provided by S3. So users don't have to worry about whether the underlying service supports, they only need to pay attention to what methods OpenDAL provides. And the good news is OpenDAL always provides interfaces in a unified way. ^_^

### Others

In addition to the many practical layers mentioned above, OpenDAL also provides support for corresponding observability and fine-grained control layers of data access for various scenarios, such as:

- [Metrics Layer](https://docs.rs/opendal/latest/opendal/layers/struct.MetricsLayer.html): add [metrics](https://docs.rs/metrics/) for every operations;
- [Tracing Layer](https://docs.rs/opendal/latest/opendal/layers/struct.TracingLayer.html): add [tracing](https://docs.rs/tracing/) for every operations;
- [Prometheus Layer](https://docs.rs/opendal/latest/opendal/layers/struct.PrometheusLayer.html): add [prometheus](https://docs.rs/prometheus) for every operations;
- [Minitrace Layer](https://docs.rs/opendal/latest/opendal/layers/struct.MinitraceLayer.html): add [minitrace](https://docs.rs/minitrace/) for every operations;
- [Immutable Index Layer](https://docs.rs/opendal/latest/opendal/layers/struct.ImmutableIndexLayer.html): add an immutable in-memory index for underlying storage services;
- [Throttle Layer](https://docs.rs/opendal/latest/opendal/layers/struct.ThrottleLayer.html): add a bandwidth rate limiter to the underlying services;
- [Await Tree Layer](https://docs.rs/opendal/latest/opendal/layers/struct.AwaitTreeLayer.html): add a instrument await-tree for actor-based applications to the underlying services;
- [Async Backtrace Layer](https://docs.rs/opendal/latest/opendal/layers/struct.AsyncBacktraceLayer.html): add efficient, logical stack traces of async functions for the underlying services;
- [Madsim Layer](https://docs.rs/opendal/latest/opendal/layers/struct.MadsimServer.html): add deterministic simulation for async operations;
- [Chaos Layer](https://docs.rs/opendal/latest/opendal/layers/struct.ChaosLayer.html): inject chaos into underlying services for robustness test.

In the end, this article serves as an introductory tutorial to introduce the techniques of using various layers in OpenDAL. You can refer to https://docs.rs/opendal/latest/opendal/layers/index.html to learn more about the layers provided in OpenDAL. Maybe there is an integrated solution you are looking for!
