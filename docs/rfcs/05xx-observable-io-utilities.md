- Proposal Name: observable-io-utilities
- Start Date: 2022-08-29
- RFC PR: [datafuselabs/opendal#5xx](https://github.com/datafuselabs/opendal/pull/05xx)
- Tracking Issue: [datafuselabs/opendal#05xx](https://github.com/datafuselabs/opendal/issues/05xx)

# Summary

Improve observability to io utilities like `Reader` and `DirStreamer`, by wrapping them into layers with functionality of logging, metrics and tracing.

# Motivation

Users demand stronger and more complete logging, tracing and metrics features on IO operators. `OpenDAL` recently resorted its observability functionalities into different layers, but those layers will still return the same utilities as they were.

# Guide-level explanation

Users have to do nothing to take advantage of those features, what they need to do is only to decorate their operators with layers and the operators will handle the rest.

For example, if users activated the logging layer, the layer will then wrap the IO utilities returned from inner layer up with logging functionalities.

```rust
let operator = Operator::from_env(Scheme::S3)?
                .layer(LoggingLayer)    // add logging to `Reader` and `DirStreamer`
                .layer(MetricsLayer)    // add metrics to `Reader` and `DirStreamer`
                .layer(TracingLayer)    // add tracing to `Reader` and `DirStreamer`
```

# Reference-level explanation

The abstraction of layers on `Accessor`s is clear and painless. This RFC extends the abstraction of layers, making them returning wrapped `Reader`s and `DirStreamer`s with their observability functionalities.

```rust
struct LoggingReader {
    inner: BytesReader
}

impl LoggingReader {
    pub(crate) fn from_reader(reader: BytesReader) -> Self {
        Self {
            inner: reader
        }
    }
}

// impl AsyncRead and BytesReader will implement automatically
impl AsyncRead for LoggingReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        debug!("Polling reader in cx: {:?}", cx);
        self.inner.poll_read(cs, buf).map(|v|)
    }
    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>]
    ) -> Poll<Result<usize>> { 
        self.inner.poll_read_vectored(cx, bufs)
    }
}

#[async_trait]
impl Accessor for LoggingAccessor {
    asnyc fn read(&self, args: &OpRead) -> Result<LoggingReader> {
        // logging
    }
}
```

- Its interaction with other features is clear.
- It is reasonably clear how the feature would be implemented.
- Corner cases are dissected by example.

The section should return to the examples given in the previous section and explain more fully how the detailed proposal makes those examples work.

# Drawbacks

Why should we *not* do this?

# Rationale and alternatives

- Why is this design the best in the space of possible designs?
- What other designs have been considered, and what is the rationale for not choosing them?
- What is the impact of not doing this?

# Prior art

Discuss prior art, both the good and the bad, in relation to this proposal.
A few examples of what this can include are:

- What lessons can we learn from what other communities have done here?

This section is intended to encourage you as an author to think about the lessons from other communities provide readers of your RFC with a fuller picture.
If there is no prior art, that is fine - your ideas are interesting to us, whether they are brand new or an adaptation from other projects.

# Unresolved questions

- What parts of the design do you expect to resolve through the RFC process before this gets merged?
- What related issues do you consider out of scope for this RFC that could be addressed in the future independently of the solution that comes out of this RFC?

# Future possibilities

Think about what the natural extension and evolution of your proposal would be and how it would affect the opendal. Try to use this section as a tool to more fully consider all possible interactions with the project in your proposal.

Also, consider how this all fits into the roadmap for the project.

This is also a good place to "dump ideas", if they are out of scope for the
RFC, you are writing but otherwise related.

If you have tried and cannot think of any future possibilities,
you may state that you cannot think of anything.

Note that having something written down in the future-possibilities section
is not a reason to accept the current or a future RFC; such notes should be
in the section on motivation or rationale in this or subsequent RFCs.
The section merely provides additional information.
