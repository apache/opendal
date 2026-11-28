# Unreleased

## `opendal_capability` ABI change

`opendal_capability` now exposes every core capability. The 57 fields
released in C binding 0.47.8 (OpenDAL v0.59.5) keep their names, types, and
order, so their offsets are unchanged, and the new fields follow them. On
Linux x86-64, the struct grew from 104 to 152 bytes.

`opendal_operator_info_get_capability` now writes the capability into
caller-provided storage instead of returning it by value, like the other
functions described in [Output parameters](README.md#output-parameters):

```c
// Before
opendal_capability cap = opendal_operator_info_get_capability(info);

// After
opendal_capability cap;
opendal_operator_info_get_capability(info, &cap);
```

New fields are only appended to `opendal_capability`, so source that
initializes it by position keeps its meaning when rebuilt. Prefer designated
initializers, which do not depend on field order:

```c
opendal_capability cap = { .stat = true, .read = true };
```

These changes break binary compatibility. Rebuild C and Zig consumers using
`opendal.h` and `libopendal_c` from the same OpenDAL source version. Mixing
binaries built against an older header with the new library is unsupported.

Later releases keep this field order but can still break binary
compatibility. Appending a field can increase `sizeof(opendal_capability)`, and
`opendal_operator_info_get_capability` writes the whole struct into
caller-provided storage, so a caller built with an older header may allocate
too little space. C and Zig consumers must keep `opendal.h` and `libopendal_c`
on the same version in every upgrade.

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
