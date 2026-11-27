# Unreleased

## `opendal::Capability` exposes every core capability

`opendal::Capability` is now generated from core and gains every capability
that core defines (for example `compose`, `restore`, `list_with_glob`, and the
`*_with_if_version_match` flags).

The 32 members that existed before (`stat` through `shared`) keep their names,
types, and declaration order, so positional brace-initialization such as
`opendal::Capability caps{false, false, false, true};` still sets `caps.read`.
New members are appended after `shared`.

The object layout changed. On 64-bit targets `sizeof(opendal::Capability)` grew
from 56 to 152 bytes.

**Migration:**

Rebuild every C++ object that includes OpenDAL headers against the headers and
library from the same OpenDAL source version. Mixing objects compiled against
the old headers with the new library is unsupported.
