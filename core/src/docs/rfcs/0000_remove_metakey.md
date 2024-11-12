- Proposal Name: `remove_metakey`
- Start Date: 2024-11-12
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Remove the `Metakey` concept from OpenDAL and replace it with a simpler and more predictable metadata handling mechanism.

# Motivation

The current `Metakey` design has several issues:

1. Performance Impact: Users often unknowingly trigger expensive operations (e.g., using `Metakey::Full` causes additional stat calls)
2. Usability Issues: Users frequently attempt to access metadata that wasn't explicitly requested
3. API Confusion: Overlap between `Metakey::Version` and the new `version(bool)` parameter
4. Implementation Complexity: Service developers find it challenging to implement `Metakey` correctly

The expected outcome is a simpler, more predictable API that prevents common mistakes and improves performance by default.

# Guide-level explanation

Instead of using `Metakey` to specify which metadata fields to fetch, services will now declare their metadata capabilities upfront through a new `MetadataCapability` struct:

```rust
let entries = op.list("path").await?;
for entry in entries {
    if op.metadata_capability().content_type {
        println!("Content-Type: {}", entry.metadata().content_type());
    }
}
```

If users need additional metadata not provided by `list`:

```rust
let entries = op.list("path").await?;
for entry in entries {
    let mut meta = entry.metadata();
    if !op.metadata_capability().etag {
        meta = op.stat(&entry.path()).await?;
    }
    println!("Content-Type: {}", meta.etag());
}
```

For existing OpenDAL users, the main changes are:

- Remove all `metakey()` calls from their code
- Use `metadata_capability()` to check available metadata
- Explicitly call `stat()` when needed

# Reference-level explanation

The implementation involves:

1. Remove the `Metakey` enum
2. Add new `MetadataCapability` struct:
```rust
pub struct MetadataCapability {
    pub content_length: bool,
    pub content_type: bool,
    pub last_modified: bool,
    pub etag: bool,
    pub mode: bool,
    pub version: bool,
    ...
}
```

3. Add method to Operator to query capabilities:
```rust
impl Operator {
    pub fn metadata_capability(&self) -> MetadataCapability;
}
```

4. Modify list operation to never perform implicit stat calls
5. Update all service implementations to declare their metadata capabilities

Each service implementation will need to:
- Remove `Metakey` handling logic
- Implement `metadata_capability()` to accurately reflect what metadata they provide by default
- Ensure list operations return metadata that's always available without extra API calls

# Drawbacks

- Breaking change for existing users
- Loss of fine-grained control over metadata fetching
- Potential increased API calls if users need multiple metadata fields

# Rationale and alternatives

This design is superior because:
- Prevents performance pitfalls by default
- Makes metadata availability explicit
- Simplifies service implementation
- Provides clearer mental model

Alternatives considered:
1. Keep `Metakey` but make it more restrictive
2. Add warnings for potentially expensive operations
3. Make stat calls async/lazy

Not making this change would continue the current issues of performance problems and API misuse.

# Prior art

None

# Unresolved questions

None

# Future possibilities

- Add metadata prefetching optimization
- Add metadata caching layer
- Support for custom metadata fields
- Automated capability detection
