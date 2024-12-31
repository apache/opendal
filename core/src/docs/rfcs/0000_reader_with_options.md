- Proposal Name: `reader_with_options`
- Start Date: 2024-12-31
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Add `if_modified_since` and `if_unmodified_since` options to OpenDAL's `reader_with` API.

# Motivation

OpenDAL currently supports conditional `reader_with` operations based only on `version`. However, many storage services also 
support conditional operations based on modification time.

Adding these options will:

- Provide more granular control over read operations.
- Align OpenDAL with features provided by modern storage services, meeting broader use cases.

# Guide-level explanation

Two new options will be added to the `reader_with` API:

## `if_modified_since`

Return the content if it has been modified since the specified time; otherwise, 
an error kind `ErrorKind::ConditionNotMatch` will be returned:

```rust
use chrono::{Duration, Utc};

let last_check = Utc::now() - Duration::seconds(3600); // 1 hour ago
let reader = op.reader_with("path/to/file")
    .if_modified_since(last_check)
    .await?;
```


## `if_unmodified_since` 

Return the content if it has NOT been modified since the specified time; otherwise, 
an error kind `ErrorKind::ConditionNotMatch` will be returned:

```rust
use chrono::{Duration, Utc};

let timestamp = Utc::now() - Duration::seconds(86400); // 24 hours ago
let reader = op.reader_with("path/to/file")
    .if_unmodified_since(timestamp)
    .await?;
```


# Reference-level explanation

The main implementation will include:

1. Add new fields(`if_modified_since`, `if_unmodified_since`) to `OpRead`.

2. Add new capability flags:
```rust
pub struct Capability {
    // ... other fields
    pub read_with_if_modified_since: bool,
    pub read_with_if_unmodified_since: bool,
}
```

3. implement `if_modified_since`, `if_unmodified_since` for the underlying storage service.

# Drawbacks

- Add complexity to the API

# Rationale and alternatives

- Follows existing OpenDAL patterns for conditional operations

# Prior art

None

# Unresolved questions

None

# Future possibilities

None
