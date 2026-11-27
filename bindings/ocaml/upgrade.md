# Upgrade Guide

## Unreleased

### Breaking Changes

#### `Operator.Metadata` gains accessors and the `entry_mode` type

`Operator.Metadata` is now generated from core. It keeps every existing
accessor with the same name and type, and adds:

- `type entry_mode = File | Dir | Unknown` and `mode : metadata -> entry_mode`.
- `is_current : metadata -> bool option` and `is_deleted : metadata -> bool`.
- `cache_control`, `content_encoding`, `content_language`, and `version`, all
  `metadata -> string option`.
- `user_metadata : metadata -> (string * string) list option`.

Code that only calls `Operator.Metadata.*` keeps compiling. The new items break
code that must provide the whole signature or that opens the module:

- A module ascribed to `module type of Opendal.Operator.Metadata`, such as a
  mock or a functor argument, no longer matches, because it lacks the new
  accessors and `entry_mode`.
- A wrapper ascribed to `module type of Opendal.Operator` that defines its own
  `Metadata` module from scratch no longer matches, for the same reason.
- After `open Opendal.Operator.Metadata`, the new names (`mode`, `version`,
  `is_current`, `is_deleted`, `cache_control`, `content_encoding`,
  `content_language`, `user_metadata`) and constructors (`File`, `Dir`,
  `Unknown`) shadow earlier definitions with the same names.

**Migration:**

Include the generated module and override only the accessors you replace:

```ocaml
module Mock_metadata : module type of Opendal.Operator.Metadata = struct
  include Opendal.Operator.Metadata

  let is_file _ = true
end
```

Instead of opening `Opendal.Operator.Metadata`, qualify the accessors, for
example `Opendal.Operator.Metadata.is_file meta`, or rename your own
definitions that clash with the new names.
