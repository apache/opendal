# Metadata

`Metadata` is an owned, immutable description of a storage entry. It does not
borrow from a response, lister, reader, or operation, so callers may retain it
for any lifetime. Clones may share immutable storage internally. Consuming one
clone through `Metadata::into_builder` and building a modified value does not
change any other clone.

## Construction and access

Callers construct metadata with `Metadata::builder(mode)` and finish it with
`MetadataBuilder::build`. Existing metadata can be consumed with
`Metadata::into_builder` to create a modified value.

User metadata is exposed through a borrowed `UserMetadata` view. The view
supports lookup, length checks, and iteration without requiring a `HashMap`
allocation. An absent user-metadata map and an explicitly supplied empty map
remain distinguishable.

Rust metadata and finalized operation arguments use compact blocks whose
encoded size, including indexes, must not exceed `u16::MAX` bytes. Construction
panics when a block exceeds this bound. Services and callers should enforce
their own smaller protocol limits before reaching this representation boundary.

## Content length

Every non-deleted file `Metadata` returned through an `Operator` contains an
explicit content length for the complete object. An explicit zero represents
an empty file. Directory metadata reports zero.

Raw services and intermediate layers may temporarily construct metadata without
a content length. Before exposing a result, the completion layer applies these
rules:

- `stat` and read-response metadata fail with `Unexpected` when file metadata
  still lacks a length.
- list entries may obtain a missing length by statting that entry; a file that
  still lacks a length fails with `Unexpected`.
- write completion fills a missing or zero response length from the number of
  bytes written when that value is authoritative.
- copy completion uses the source-length hint, the source length discovered by
  a segmented copier, or the successful byte progress reported while copying.
  It does not issue a target `stat`; a copy that returns file metadata without
  enough information to determine its result length fails with `Unexpected`.

Delete markers are identity records rather than readable file results and do
not require content length completion.

## Operation boundary

Public operation options are mutable logical construction values. Before
OpenDAL freezes them into raw `Op*` arguments, it resolves logical conditions
such as `if_not_changed` against the composed service capability. Raw arguments
contain only the primitive conditions that a service must execute and do not
retain the source `Metadata`.

## Historical decisions

- [RFC 8194: Compact metadata and operation arguments](https://github.com/apache/opendal/blob/main/core/core/src/docs/rfcs/8194_compact_metadata_and_operation_args.md)
- [Tracking issue 8195](https://github.com/apache/opendal/issues/8195)
