# Unreleased

## `opendal_capability` ABI change

`opendal_capability` now exposes every core capability. On Linux x86-64,
the struct grew from 104 to 152 bytes and field offsets changed.

Rebuild C and Zig consumers using `opendal.h` and `libopendal_c` from the
same OpenDAL source version. Mixing binaries built against the old layout
with the new library is unsupported.

# Upgrade to v0.45

Change to use `Cmake` to build the C binding.

# Upgrade to v0.42

The naming convention for C binding has been altered.

## Struct Naming

Renaming certain struct names for consistency.

- `opendal_operator_ptr` => `opendal_operator`
- `opendal_blocking_lister` => `opendal_lister`
- `opendal_list_entry` => `opendal_entry`

## API Naming

We've eliminated the `blocking_` prefix from our API because C binding doesn't currently support async. In the future, we plan to introduce APIs such as `opendal_operator_async_write`.

- `opendal_operator_blocking_write` => `opendal_operator_write`
- `opendal_operator_blocking_read` => `opendal_operator_read`
