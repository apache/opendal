// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::*;
use std::hash::{BuildHasher, Hasher};

// a representation of root path.
// Don't get confused with file system root. Because services can define services' root.
// For example, when users create an `Operator` with a root like `/root/dir/`, the root path is `/root/dir/`.
const ROOT_PATH: &str = "/";
const PATH_SEPARATOR: &str = "/";
const PATH_SEPARATOR_CHAR: char = '/';

/// Build an absolute path by joining root and path, stripping the leading `/` from root.
///
/// # Conformance
///
/// - Users MUST provide a root that follows a format like `/`, `/root/dir/`.
/// - Output will follow a format like `path/to/root/path`.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::build_absolute_path;
/// #
/// assert_eq!(
///     build_absolute_path("/root/", "dir/"),
///     "root/dir/",
///     "input dir with root /root/"
/// );
/// assert_eq!(
///     build_absolute_path("/root/", "file.txt"),
///     "root/file.txt",
///     "input file with root /root/"
/// );
/// assert_eq!(
///     build_absolute_path("/", "dir/"),
///     "dir/",
///     "input dir with root /"
/// );
/// ```
pub fn build_absolute_path(root: &str, path: &str) -> String {
    debug_assert!(root.starts_with('/'), "root must start with /");
    debug_assert!(root.ends_with('/'), "root must end with /");

    let p = root[1..].to_string();

    if path == ROOT_PATH {
        p
    } else {
        debug_assert!(!path.starts_with('/'), "path must not start with /");
        p + path
    }
}

/// Build an absolute path by joining root and path, preserving the leading `/`.
///
/// # Conformance
///
/// - Users MUST provide a root that follows a format like `/`, `/abc/def/`.
/// - Output will follow a format like `/path/to/root/path`.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::build_rooted_absolute_path;
/// #
/// assert_eq!(
///     build_rooted_absolute_path("/root/", "dir/"),
///     "/root/dir/",
///     "input dir with root /root/"
/// );
/// assert_eq!(
///     build_rooted_absolute_path("/", "dir/"),
///     "/dir/",
///     "input dir with root /"
/// );
/// assert_eq!(
///     build_rooted_absolute_path("/root/", "file.txt"),
///     "/root/file.txt",
///     "input file with root /root/"
/// );
/// assert_eq!(
///     build_rooted_absolute_path("/", ""),
///     "/",
///     "input root path with root /"
/// );
/// ```
pub fn build_rooted_absolute_path(root: &str, path: &str) -> String {
    debug_assert!(root.starts_with('/'), "root must start with /");
    debug_assert!(root.ends_with('/'), "root must end with /");

    let p = root.to_string();

    if path == ROOT_PATH {
        p
    } else {
        debug_assert!(!path.starts_with('/'), "path must not start with /");
        p + path
    }
}

/// Build a relative path from a rooted absolute path by stripping the root prefix.
///
/// # Conformance
///
/// - Users MUST provide a root that follows a format like `/`, `/root/dir/`.
/// - Output will follow a format like `path/to/file` stripping the root prefix.
///   The output will not have a leading `/`.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::build_relative_path;
/// #
/// assert_eq!(
///     build_relative_path("/root/", "/root/file.txt"),
///     "file.txt",
///     "input absolute file with root /root/"
/// );
/// assert_eq!(
///     build_relative_path("/root/", "/root/dir/file.txt"),
///     "dir/file.txt",
///     "input file path with root /root/"
/// );
/// assert_eq!(
///     build_relative_path("/", "/dir"),
///     "dir",
///     "input absolute file with root /"
/// );
/// assert_eq!(
///     build_relative_path("/", "dir"),
///     "dir",
///     "input file with root /"
/// );
/// assert_eq!(
///     build_relative_path("/root/", "/root/file.txt"),
///     "file.txt",
///     "input file path with root /root/"
/// );
/// ```
pub fn build_relative_path(root: &str, path: &str) -> String {
    debug_assert!(root != path, "build relative path with root is invalid");

    if path.starts_with(ROOT_PATH) {
        debug_assert!(
            path.starts_with(root),
            "path {path} doesn't start with root {root}"
        );
        path[root.len()..].to_string()
    } else {
        debug_assert!(
            path.starts_with(&root[1..]),
            "path {path} doesn't start with root {root}"
        );
        path[root.len() - 1..].to_string()
    }
}

/// Normalize a user-provided path for services.
///
/// OpenDAL core's `Operator` normalizes paths before services process normalized paths in `Service` trait.
/// e.g., when users call `op.read("///root/dir/")`, OpenDAL normalizes the path to `root/dir/`
/// before calling `Service::read` in services.
///
/// # Path conventions for OpenDAL users
///
/// - Paths ending with `/` represent directories.
/// - Otherwise, paths represent files.
///
/// # Normalize Rules
///
/// - Strip leading `/`: `/root` => `root`
/// - Collapse double slashes `//` in a path: `root///dir` => `root/dir`
/// - Remove `.` segments:
///    - `./root/./dir` => `root/dir`
///    - `./` => `/`
/// - Preserve trailing `/`: `dir/` => `dir/`
/// - Empty path becomes `/`: `` => `/`
/// - Preserve content within path components (including whitespace):
///      - `root/file   ` => `root/file   `
///      - `  root/dir` => `  root/dir`
///      - ` root/dir ` => ` root/dir `
///
///   This aligns with POSIX, URI, and object-store conventions where
///   whitespace is significant content — `file` and `file ` are
///   distinct objects.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::normalize_path;
/// #
/// assert_eq!(normalize_path("file"), "file", "normalize file path");
/// assert_eq!(normalize_path("dir/"), "dir/", "normalize dir path");
/// assert_eq!(
///     normalize_path("/root/dir/"),
///     "root/dir/",
///     "absolute dir path with root /root/"
/// );
/// assert_eq!(normalize_path("///root//dir"), "root/dir");
/// assert_eq!(
///     normalize_path("./root/./dir"),
///     "root/dir",
///     "removes dot segment"
/// );
/// assert_eq!(normalize_path(""), "/", "empty path");
/// ```
pub fn normalize_path(path: &str) -> String {
    let path = path.trim_start_matches(PATH_SEPARATOR);

    if path.is_empty() {
        return ROOT_PATH.to_string();
    }

    let has_trailing = path.ends_with(PATH_SEPARATOR);

    let mut p = path
        .split(PATH_SEPARATOR_CHAR)
        .filter(|v| !v.is_empty() && *v != ".")
        .collect::<Vec<&str>>()
        .join(PATH_SEPARATOR);

    if p.is_empty() {
        return ROOT_PATH.to_string();
    }

    if has_trailing {
        p.push_str(PATH_SEPARATOR);
    }

    p
}

/// Normalize a root path to the style `/root/dir/`.
///
/// # Normalize Rules
///
/// - Strip excessive leading `/` but keep one: `///root` => `/root/`
/// - Ensure leading `/` presence: `root/` => `/root/`
/// - Collapse internal `//`: `root///dir` => `/root/dir/`
/// - Remove `.` segments: `./workspace/./root/` => `/workspace/root/`
/// - Ensure trailing `/` presence: `/root` => `/root/`
/// - Empty path becomes `/`: `` => `/`
/// - Preserves content within path components including whitespace: `workspace/file ` => `workspace/file `
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::normalize_root;
/// #
/// assert_eq!(normalize_root("root/"), "/root/", "dir path");
/// assert_eq!(
///     normalize_root("///root//dir"),
///     "/root/dir/",
///     "abs dir path with extra /"
/// );
/// assert_eq!(normalize_root("."), "/", "only dot");
/// assert_eq!(
///     normalize_root("./workspace/./report/"),
///     "/workspace/report/",
///     "dot within path"
/// );
/// assert_eq!(
///     normalize_root("/root/dir v1"),
///     "/root/dir v1/",
///     "respects whitespace in path component"
/// );
/// ```
pub fn normalize_root(v: &str) -> String {
    let mut v = v
        .split(PATH_SEPARATOR)
        .filter(|v| !v.is_empty() && *v != ".")
        .collect::<Vec<&str>>()
        .join(PATH_SEPARATOR);
    if !v.starts_with(PATH_SEPARATOR) {
        v.insert_str(0, PATH_SEPARATOR);
    }
    if !v.ends_with(PATH_SEPARATOR) {
        v.push_str(PATH_SEPARATOR);
    }
    v
}

/// Get basename from path.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::get_basename;
/// assert_eq!(get_basename("root/dir/file.txt"), "file.txt", "file absolute path");
/// assert_eq!(get_basename("root/dir/"), "dir/");
/// assert_eq!(get_basename("/"), "/", "dir root");
/// ```
pub fn get_basename(path: &str) -> &str {
    // Handle root case
    if path == ROOT_PATH {
        return ROOT_PATH;
    }

    // Handle file case
    if !path.ends_with(PATH_SEPARATOR) {
        return path
            .split(PATH_SEPARATOR_CHAR)
            .next_back()
            .expect("file path without name is invalid");
    }

    // The idx of second `/` if path in reserve order.
    // - `abc/` => `None`
    // - `abc/def/` => `Some(3)`
    let idx = path[..path.len() - 1].rfind(PATH_SEPARATOR).map(|v| v + 1);

    match idx {
        Some(v) => {
            let (_, name) = path.split_at(v);
            name
        }
        None => path,
    }
}

/// Get parent from path.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::get_parent;
/// assert_eq!(get_parent("root/dir/file.txt"), "root/dir/", "get parent of a file's path");
/// assert_eq!(get_parent("root/dir/"), "root/", "get parent of a dir's path");
/// assert_eq!(get_parent("/root/dir/file.txt"), "/root/dir/", "get parent of a file's path");
/// assert_eq!(get_parent("/root/dir/"), "/root/", "get parent of a dir's path");
/// assert_eq!(get_parent("dir"), "/");
/// assert_eq!(get_parent("/"), "/", "dir root");
/// ```
pub fn get_parent(path: &str) -> &str {
    if path == ROOT_PATH {
        return ROOT_PATH;
    }

    if !path.ends_with(PATH_SEPARATOR) {
        // The idx of first `/` if path in reserve order.
        // - `abc` => `None`
        // - `abc/def` => `Some(3)`
        let idx = path.rfind(PATH_SEPARATOR);

        return match idx {
            Some(v) => {
                let (parent, _) = path.split_at(v + 1);
                parent
            }
            None => PATH_SEPARATOR,
        };
    }

    // The idx of second `/` if path in reserve order.
    // - `abc/` => `None`
    // - `abc/def/` => `Some(3)`
    let idx = path[..path.len() - 1].rfind(PATH_SEPARATOR).map(|v| v + 1);

    match idx {
        Some(v) => {
            let (parent, _) = path.split_at(v);
            parent
        }
        None => PATH_SEPARATOR,
    }
}

// Sets the size of random generated postfix for random file names
const RANDOM_TMP_PATH_POSTFIX_LENGTH: usize = 8;
// Allowed characters for choices in a random-generated char
const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
const CHARS_LENGTH: u64 = CHARS.len() as u64;

/// Build a temporary path of a file path.
///
/// `build_tmp_path_of` appends a dot following a random generated postfix.
/// Don't use it with a path to a folder.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::build_tmp_path_of;
/// #
/// let name = "a file path in a directory";
/// let tmp = build_tmp_path_of("report.txt");
/// assert!(tmp.starts_with("report.txt."), "use file name as prefix");
/// ```
#[inline]
pub fn build_tmp_path_of(path: &str) -> String {
    let name = get_basename(path);
    let mut buf = String::with_capacity(name.len() + RANDOM_TMP_PATH_POSTFIX_LENGTH);
    buf.push_str(name);
    buf.push('.');

    // Uses `std` for the random number generator instead of external crates.
    //
    // `RandomState::new` builds a hasher that generates a different random sequence each time.
    // Calling `RandomState::new` each time produces a `RandomState` from
    // a per-thread pseudo seed pools that Rust manages.
    // The default hasher, `SipHasher13`, has some notable properties:
    //
    // 1. `fastrand` is roughly 10x faster than `SipHasher13`, but it adds an extra dependency.
    // 2. While `fastrand` is faster, `SipHasher13` is fast enough for our needs since
    //    we're only generating a few characters.
    // 3. This is not a cryptographically secure pseudorandom number generator (CSPRNG).
    //
    // If we need stronger randomness in the future, we can:
    // 1. Increase the output length.
    // 2. Use the `getrandom` crate to source randomness from the OS.
    for _ in 0..RANDOM_TMP_PATH_POSTFIX_LENGTH {
        let random = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        let choice: usize = (random % CHARS_LENGTH).try_into().unwrap();
        buf.push(CHARS[choice] as char);
    }

    buf
}

/// Validate if a path matches EntryMode by OpenDAL path convention.
///
/// # Examples
///
/// ```
/// # use opendal_core::raw::validate_path;
/// # use opendal_core::EntryMode;
/// #
/// assert!(validate_path("file.txt", EntryMode::FILE), "is a file");
/// assert!(!validate_path("dir/", EntryMode::FILE), "dir is not a file with FILE mode");
/// assert!(validate_path("dir/", EntryMode::DIR), "is a directory");
/// assert!(!validate_path("dir/", EntryMode::FILE), "dir is not a file");
/// assert!(!validate_path("file.txt", EntryMode::Unknown), "is not a valid mode");
/// assert!(!validate_path("dir/", EntryMode::Unknown), "is not a valid mode");
/// ```
#[inline]
pub fn validate_path(path: &str, mode: EntryMode) -> bool {
    debug_assert!(!path.is_empty(), "input path should not be empty");

    match mode {
        EntryMode::FILE => !path.ends_with(ROOT_PATH),
        EntryMode::DIR => path.ends_with(ROOT_PATH),
        EntryMode::Unknown => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Case {
        name: &'static str,
        input: &'static str,
        expect: &'static str,
    }

    struct RootCase {
        name: &'static str,
        root: &'static str,
        expect: &'static str,
    }

    #[test]
    fn test_normalize_path() {
        let cases = vec![
            Case {
                name: "root path",
                input: "/",
                expect: "/",
            },
            Case {
                name: "root path with extra /",
                input: "///",
                expect: "/",
            },
            Case {
                name: "absolute file path",
                input: "/abc/def",
                expect: "abc/def",
            },
            Case {
                name: "absolute file path with extra /",
                input: "///abc/def",
                expect: "abc/def",
            },
            Case {
                name: "absolute dir path with extra /",
                input: "///abc/def/",
                expect: "abc/def/",
            },
            Case {
                name: "file path contains ///",
                input: "abc///def",
                expect: "abc/def",
            },
            Case {
                name: "dir path contains ///",
                input: "abc///def///",
                expect: "abc/def/",
            },
            Case {
                name: "file with trailing whitespace",
                input: "abc/def   ",
                expect: "abc/def   ",
            },
            Case {
                name: "file with leading whitespace",
                input: "  abc/def",
                expect: "  abc/def",
            },
            Case {
                name: "whitespace preserved",
                input: " a/b ",
                expect: " a/b ",
            },
            Case {
                name: "whitespace only preserved as file name",
                input: "   ",
                expect: "   ",
            },
            Case {
                name: "dot dir becomes root",
                input: "./",
                expect: "/",
            },
            Case {
                name: "only dot",
                input: ".",
                expect: "/",
            },
            Case {
                name: "dot in middle",
                input: "a/./b/./c",
                expect: "a/b/c",
            },
            Case {
                name: "dot with trailing slash",
                input: "a/./b/",
                expect: "a/b/",
            },
        ];

        for case in cases {
            assert_eq!(normalize_path(case.input), case.expect, "{}", case.name);
        }
    }

    #[test]
    fn test_normalize_root() {
        let cases = vec![
            RootCase {
                name: "empty path",
                root: "",
                expect: "/",
            },
            RootCase {
                name: "root path",
                root: "/",
                expect: "/",
            },
            RootCase {
                name: "root path with extra /",
                root: "///",
                expect: "/",
            },
            RootCase {
                name: "abs dir path",
                root: "/abc/def/",
                expect: "/abc/def/",
            },
            RootCase {
                name: "abs file path with extra /",
                root: "///abc/def",
                expect: "/abc/def/",
            },
            RootCase {
                name: "abs dir path with extra /",
                root: "///abc/def/",
                expect: "/abc/def/",
            },
            RootCase {
                name: "dir path contains ///",
                root: "abc///def///",
                expect: "/abc/def/",
            },
            RootCase {
                name: "dot segment removed",
                root: "./data/./root/",
                expect: "/data/root/",
            },
            RootCase {
                name: "dot with path",
                root: "./abc",
                expect: "/abc/",
            },
        ];

        for case in cases {
            assert_eq!(normalize_root(case.root), case.expect, "{}", case.name);
        }
    }

    #[test]
    fn test_get_basename() {
        let cases = vec![
            Case {
                name: "file relative path",
                input: "bar/baz.txt",
                expect: "baz.txt",
            },
            Case {
                name: "file walk",
                input: "foo/bar/baz",
                expect: "baz",
            },
            Case {
                name: "dir relative path",
                input: "bar/baz/",
                expect: "baz/",
            },
            Case {
                name: "dir walk",
                input: "foo/bar/baz/",
                expect: "baz/",
            },
        ];

        for case in cases {
            let actual = get_basename(case.input);
            assert_eq!(actual, case.expect, "{}", case.name);
        }
    }

    #[test]
    fn test_get_parent() {
        assert_eq!(get_parent("foo/bar/baz/"), "foo/bar/", "dir walk")
    }

    #[test]
    fn test_build_absolute_path() {
        assert_eq!(
            build_absolute_path("/", "/"),
            "",
            "input root path with root /"
        );
        assert_eq!(
            build_absolute_path("/", ""),
            "",
            "input empty file path with root /"
        );
        assert_eq!(
            build_absolute_path("/", "def"),
            "def",
            "input file with root /"
        );
    }

    #[test]
    fn test_build_rooted_absolute_path() {
        assert_eq!(
            build_rooted_absolute_path("/", ""),
            "/",
            "input empty with root /"
        );

        assert_eq!(
            build_rooted_absolute_path("/", "def/"),
            "/def/",
            "input dir with root /"
        );
    }

    #[test]
    fn test_build_tmp_path_of() {
        let actual = build_tmp_path_of("folder/example.txt");

        assert_eq!(
            actual.len(),
            "example.txt.".len() + 8, // See RANDOM_TMP_PATH_POSTFIX_SIZE
            "a file path in a directory: got `{actual}`, but expect `example.txt.`"
        )
    }
}
