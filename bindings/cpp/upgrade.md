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

## `opendal::Metadata` gains `content_language` and `user_metadata`

`opendal::Metadata` is now generated from core and gains `content_language`
(`ContentLanguage()`) and `user_metadata` (`UserMetadata()`, a
`std::optional<std::unordered_map<std::string, std::string>>`). Core's
`checksum` is not exposed yet.

The 12 members that existed before (`type` through `is_deleted`) keep their
names, types, default values, and declaration order, so a C++17 positional
initializer such as `opendal::Metadata meta{opendal::EntryMode::FILE, 42};`
keeps its meaning. New members are appended after `is_deleted` and default to
`std::nullopt`, so a positional initializer of all 12 existing members still
compiles cleanly with `-Wextra -Werror`.

A structured binding decomposes `opendal::Metadata` into all of its members, so
a binding with 12 names, such as `const auto& [m1, m2, ..., m12] = metadata;`,
no longer compiles in C++17 or C++20.

The object layout changed. `sizeof(opendal::Metadata)` grew from 320 to 424
bytes on Linux x86-64 with GCC 13, and from 264 to 344 bytes on macOS arm64
with Apple clang. The offsets of the existing members did not change.

**Migration:**

Rebuild every C++ object that includes OpenDAL headers against the headers and
library from the same OpenDAL source version. Replace a structured binding of
`opendal::Metadata` with reads of the members you need by name or through
their accessors, or add two names for `content_language` and `user_metadata`.
