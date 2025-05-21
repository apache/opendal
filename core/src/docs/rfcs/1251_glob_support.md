- Proposal Name: `glob_support`
- Start Date: 2023-01-29
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#1251](https://github.com/apache/opendal/issues/1251)

# Summary

Add support for matching file paths against Unix shell style patterns (glob) in OpenDAL.

# Motivation

Glob patterns are a widely used way to filter files based on their paths. They provide a simple and intuitive syntax for matching multiple files with similar path patterns. Adding glob support to OpenDAL would enable users to easily filter and process files that match certain patterns without having to implement this functionality themselves.

Currently, users who want to filter objects based on patterns have to list all objects and then apply filters manually, which is verbose and not very intuitive. By providing native glob support, we can make this common operation more convenient and efficient.

# Guide-level explanation

With glob support, users can easily match files based on patterns. The API would be available on the `Operator` struct, allowing users to get an iterator over entries that match the provided glob pattern.

For example:

```rust
// Get an iterator over all jpeg files in the media directory and its subdirectories
let it = op.glob("media/**/*.jpg").await?;

while let Some(entry) = it.next().await? {
   do_something(&entry);
}
```

The glob syntax would support common patterns like:

- `*` - Match any sequence of non-separator characters
- `?` - Match any single non-separator character
- `**` - Match any sequence of characters including separators
- `{a,b}` - Match either a or b
- `[ab]` - Match either a or b
- `[a-z]` - Match any character in range a-z

Both blocking and non-blocking (async) versions of the API would be provided:

```rust
// Async version
pub async fn glob(&self, pattern: &str) -> Result<GlobStream>;

// Blocking version
pub fn glob_blocking(&self, pattern: &str) -> Result<GlobStreamBlocking>;
```

# Reference-level explanation

The implementation would involve:

1. Adding a dependency on a glob pattern matching library like [globset](https://crates.io/crates/globset) or [glob](https://crates.io/crates/glob)

2. Adding new methods to the `Operator` struct:

```rust
impl Operator {
    pub async fn glob(&self, pattern: &str) -> Result<GlobStream> {
        let pattern = GlobPattern::new(pattern)?;
        let lister = self.scan().await?;
        
        Ok(GlobStream::new(lister, pattern))
    }
    
    pub fn glob_blocking(&self, pattern: &str) -> Result<GlobStreamBlocking> {
        let pattern = GlobPattern::new(pattern)?;
        let lister = self.scan_blocking()?;
        
        Ok(GlobStreamBlocking::new(lister, pattern))
    }
}
```

3. Creating `GlobStream` and `GlobStreamBlocking` types to wrap the existing lister types and filter entries based on the glob pattern:

```rust
pub struct GlobStream {
    inner: ObjectStream,
    pattern: GlobPattern,
}

impl Stream for GlobStream {
    type Item = Result<Entry>;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.project().inner.poll_next(cx)) {
                Some(Ok(entry)) => {
                    if self.pattern.matches(entry.path()) {
                        return Poll::Ready(Some(Ok(entry)));
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}
```

The implementation would be built on top of the existing listing capabilities, so it would work across all services that support listing. The actual matching would happen client-side since most storage services don't natively support glob filtering.

# Drawbacks

- Adding glob support increases the API surface area slightly
- The glob matching happens client-side, so all objects still need to be listed and transferred over the network before filtering
- It may not be as efficient as server-side filtering for services that could support it natively

# Rationale and alternatives

This design builds on top of existing listing functionality, making it relatively simple to implement while still providing a useful feature. It leverages existing Rust glob libraries rather than implementing pattern matching from scratch.

Alternatives considered:

1. Not implementing this feature and letting users implement filtering manually
   - This puts the burden on users and leads to repetitive code
   - Users might implement inefficient or buggy filtering

2. Implementing server-side filtering for services that support it
   - More complex and would require service-specific implementations
   - Not all services support the same pattern syntax, leading to inconsistencies

3. Adding a more general filtering API instead of specifically glob patterns
   - Would be more flexible but also more complex
   - Glob patterns are familiar to users and cover many common use cases

Not providing glob support means users continue to write verbose code for a common operation, reducing OpenDAL's ergonomics.

# Prior art

Many file system and storage APIs provide glob or similar pattern matching capabilities:

- The [glob](https://crates.io/crates/glob) crate in Rust
- Python's [glob](https://docs.python.org/3/library/glob.html) module
- Node.js [glob](https://www.npmjs.com/package/glob) package
- Unix shells like bash with built-in glob support

Most implementations provide similar syntax, though there are some variations. We should align with established Rust patterns.

# Unresolved questions

- Should we support custom pattern matching options like case sensitivity?
- Should we provide a way to limit recursion depth when using `**` patterns?
- Should we provide additional methods for specific use cases like collecting all matches into a vector?

# Future possibilities

- If services add native support for glob filtering, we could optimize by pushing the filtering to the server side
- We could extend the API to support more advanced pattern matching like regex
- We might add bulk operations that work on glob matches, like deleting all matches 