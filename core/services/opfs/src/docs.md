## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Delete

Deleting a missing path succeeds, including when an ancestor directory is missing.
Recursive delete removes the named file or directory and its contents. For
directories, the trailing slash is optional. When the operator targets the
origin root directly, recursively deleting `/` removes its children and keeps
the root directory.

## Configuration

Use [`crate::OpfsConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.
## Example
