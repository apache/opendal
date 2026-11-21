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
use std::path::{Component, Path, PathBuf};

/// Join a normalized root and operation path, omitting the leading `/`.
///
/// Pass a root such as `/` or `/abc/def/`. An operation path of `/` addresses
/// the root itself.
pub fn build_absolute_path(root: &str, path: &str) -> String {
    debug_assert!(root.starts_with('/'), "root must start with /");
    debug_assert!(root.ends_with('/'), "root must end with /");

    let p = root[1..].to_string();

    if path == "/" {
        p
    } else {
        debug_assert!(!path.starts_with('/'), "path must not start with /");
        p + path
    }
}

/// Join a normalized root and operation path, keeping the leading `/`.
///
/// Pass a root such as `/` or `/abc/def/`. An operation path of `/` addresses
/// the root itself.
pub fn build_rooted_absolute_path(root: &str, path: &str) -> String {
    debug_assert!(root.starts_with('/'), "root must start with /");
    debug_assert!(root.ends_with('/'), "root must end with /");

    let p = root.to_string();

    if path == "/" {
        p
    } else {
        debug_assert!(!path.starts_with('/'), "path must not start with /");
        p + path
    }
}

/// Strip a normalized root prefix from a path.
///
/// Pass a root such as `/` or `/abc/def/`. The path must start with that
/// prefix and may omit its leading `/`. The returned path is relative to root.
pub fn build_relative_path(root: &str, path: &str) -> String {
    debug_assert!(root != path, "get rel path with root is invalid");

    if path.starts_with('/') {
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

/// Deprecated alias for [`build_absolute_path`].
#[deprecated(note = "use build_absolute_path instead")]
#[inline]
pub fn build_abs_path(root: &str, path: &str) -> String {
    build_absolute_path(root, path)
}

/// Deprecated alias for [`build_rooted_absolute_path`].
#[deprecated(note = "use build_rooted_absolute_path instead")]
#[inline]
pub fn build_rooted_abs_path(root: &str, path: &str) -> String {
    build_rooted_absolute_path(root, path)
}

/// Deprecated alias for [`build_relative_path`].
#[deprecated(note = "use build_relative_path instead")]
#[inline]
pub fn build_rel_path(root: &str, path: &str) -> String {
    build_relative_path(root, path)
}

/// Normalize an operation path relative to the operator's root.
///
/// Strip leading `/`, collapse repeated `/`, and remove `.` components.
/// Preserve all other components verbatim, including whitespace and `..`.
/// A trailing `/` marks a directory and is preserved. A path with no remaining
/// components becomes `/`.
pub fn normalize_path(path: &str) -> String {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return "/".to_string();
    }

    let mut p = path
        .split('/')
        .filter(|v| !v.is_empty() && *v != ".")
        .collect::<Vec<&str>>()
        .join("/");

    if p.is_empty() {
        return "/".to_string();
    }
    if path.ends_with('/') {
        p.push('/');
    }

    p
}

/// Normalize a service root to `/` or a path with one leading and trailing `/`.
///
/// Collapse repeated `/` and remove `.` components. Preserve all other
/// components verbatim, including whitespace and `..`.
pub fn normalize_root(v: &str) -> String {
    let mut v = v
        .split('/')
        .filter(|v| !v.is_empty() && *v != ".")
        .collect::<Vec<&str>>()
        .join("/");
    if !v.starts_with('/') {
        v.insert(0, '/');
    }
    if !v.ends_with('/') {
        v.push('/')
    }
    v
}

/// Join an OpenDAL path to a local filesystem root.
///
/// This function strips trailing `/` characters, so the OpenDAL root path `/`
/// resolves to `root`. It returns [`ErrorKind::NotFound`] for parent-directory
/// components, root-directory components, or Windows path prefixes.
///
/// The check is lexical. The filesystem resolves symlinks.
pub fn confined_join(root: &Path, path: &str) -> Result<PathBuf> {
    let trimmed = path.trim_end_matches('/');
    if Path::new(trimmed).components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(
            Error::new(ErrorKind::NotFound, "path escapes the configured root")
                .with_context("path", path),
        );
    }
    Ok(root.join(trimmed))
}

/// Get basename from path.
pub fn get_basename(path: &str) -> &str {
    // Handle root case
    if path == "/" {
        return "/";
    }

    // Handle file case
    if !path.ends_with('/') {
        return path
            .split('/')
            .next_back()
            .expect("file path without name is invalid");
    }

    // The idx of second `/` if path in reserve order.
    // - `abc/` => `None`
    // - `abc/def/` => `Some(3)`
    let idx = path[..path.len() - 1].rfind('/').map(|v| v + 1);

    match idx {
        Some(v) => {
            let (_, name) = path.split_at(v);
            name
        }
        None => path,
    }
}

/// Get parent from path.
pub fn get_parent(path: &str) -> &str {
    if path == "/" {
        return "/";
    }

    if !path.ends_with('/') {
        // The idx of first `/` if path in reserve order.
        // - `abc` => `None`
        // - `abc/def` => `Some(3)`
        let idx = path.rfind('/');

        return match idx {
            Some(v) => {
                let (parent, _) = path.split_at(v + 1);
                parent
            }
            None => "/",
        };
    }

    // The idx of second `/` if path in reserve order.
    // - `abc/` => `None`
    // - `abc/def/` => `Some(3)`
    let idx = path[..path.len() - 1].rfind('/').map(|v| v + 1);

    match idx {
        Some(v) => {
            let (parent, _) = path.split_at(v);
            parent
        }
        None => "/",
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

/// Validate given path is match with given EntryMode.
#[inline]
pub fn validate_path(path: &str, mode: EntryMode) -> bool {
    debug_assert!(!path.is_empty(), "input path should not be empty");

    match mode {
        EntryMode::FILE => !path.ends_with('/'),
        EntryMode::DIR => path.ends_with('/'),
        EntryMode::Unknown => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        let cases = vec![
            ("file path", "abc", "abc"),
            ("dir path", "abc/", "abc/"),
            ("empty path", "", "/"),
            ("root path", "/", "/"),
            ("root path with extra /", "///", "/"),
            ("abs file path", "/abc/def", "abc/def"),
            ("abs dir path", "/abc/def/", "abc/def/"),
            ("abs file path with extra /", "///abc/def", "abc/def"),
            ("abs dir path with extra /", "///abc/def/", "abc/def/"),
            ("file path contains ///", "abc///def", "abc/def"),
            ("dir path contains ///", "abc///def///", "abc/def/"),
            ("file with trailing whitespace", "abc/def   ", "abc/def   "),
            ("file with leading whitespace", "  abc/def", "  abc/def"),
            (
                "Unicode whitespace",
                "\u{2003}abc/def\t\n",
                "\u{2003}abc/def\t\n",
            ),
            ("whitespace-only file name", "   ", "   "),
            ("dot path", ".", "/"),
            ("dot directory path", "./", "/"),
            ("dot-only segments", "///././/.", "/"),
            ("leading and internal dots", "./abc/./def", "abc/def"),
            ("trailing dot without slash", "abc/.", "abc"),
            ("trailing dot with slash", "abc/.//", "abc/"),
            (
                "components containing dots",
                "a/../.hidden/ . /b.",
                "a/../.hidden/ . /b.",
            ),
        ];

        for (name, input, expect) in cases {
            assert_eq!(normalize_path(input), expect, "{name}")
        }
    }

    #[test]
    fn test_normalize_root() {
        let cases = vec![
            ("dir path", "abc/", "/abc/"),
            ("empty path", "", "/"),
            ("root path", "/", "/"),
            ("root path with extra /", "///", "/"),
            ("abs dir path", "/abc/def/", "/abc/def/"),
            ("abs file path with extra /", "///abc/def", "/abc/def/"),
            ("abs dir path with extra /", "///abc/def/", "/abc/def/"),
            ("dir path contains ///", "abc///def///", "/abc/def/"),
            ("dot-only root", "././", "/"),
            ("root with dot segments", "./data/./root/", "/data/root/"),
            ("root with whitespace", " data/root /", "/ data/root /"),
        ];

        for (name, input, expect) in cases {
            assert_eq!(normalize_root(input), expect, "{name}")
        }
    }

    #[test]
    fn test_confined_join_allows_relative_paths() {
        let root = Path::new("/data/root");
        for (path, expected) in [
            ("", "/data/root"),
            ("/", "/data/root"),
            (".", "/data/root"),
            ("a/b.txt", "/data/root/a/b.txt"),
            ("./a/b", "/data/root/a/b"),
            ("a/./b", "/data/root/a/b"),
            ("a..b", "/data/root/a..b"),
        ] {
            assert_eq!(
                confined_join(root, path).unwrap(),
                Path::new(expected),
                "{path:?}"
            );
        }

        assert_eq!(
            confined_join(root, "a/b/").unwrap().as_os_str(),
            root.join("a/b").as_os_str()
        );
    }

    #[test]
    fn test_confined_join_rejects_parent_and_root_components() {
        let root = Path::new("/data/root");
        for path in [
            "..",
            "../file",
            "../../file",
            "a/../../b",
            "a/..",
            "a/../",
            "/file",
            "//dir/file",
        ] {
            let error = confined_join(root, path).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::NotFound, "{path:?}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_confined_join_rejects_windows_prefix() {
        let root = Path::new(r"C:\data\root");
        let error = confined_join(root, "D:file").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_get_basename() {
        let cases = vec![
            ("file abs path", "foo/bar/baz.txt", "baz.txt"),
            ("file rel path", "bar/baz.txt", "baz.txt"),
            ("file walk", "foo/bar/baz", "baz"),
            ("dir rel path", "bar/baz/", "baz/"),
            ("dir root", "/", "/"),
            ("dir walk", "foo/bar/baz/", "baz/"),
        ];

        for (name, input, expect) in cases {
            let actual = get_basename(input);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_get_parent() {
        let cases = vec![
            ("file abs path", "foo/bar/baz.txt", "foo/bar/"),
            ("file rel path", "bar/baz.txt", "bar/"),
            ("file walk", "foo/bar/baz", "foo/bar/"),
            ("dir rel path", "bar/baz/", "bar/"),
            ("dir root", "/", "/"),
            ("dir abs path", "/foo/bar/", "/foo/"),
            ("dir walk", "foo/bar/baz/", "foo/bar/"),
        ];

        for (name, input, expect) in cases {
            let actual = get_parent(input);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_build_abs_path() {
        let cases = vec![
            ("input abs file", "/abc/", "/", "abc/"),
            ("input dir", "/abc/", "def/", "abc/def/"),
            ("input file", "/abc/", "def", "abc/def"),
            ("input abs file with root /", "/", "/", ""),
            ("input empty with root /", "/", "", ""),
            ("input dir with root /", "/", "def/", "def/"),
            ("input file with root /", "/", "def", "def"),
        ];

        for (name, root, input, expect) in cases {
            let actual = build_absolute_path(root, input);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_build_rooted_abs_path() {
        let cases = vec![
            ("input abs file", "/abc/", "/", "/abc/"),
            ("input dir", "/abc/", "def/", "/abc/def/"),
            ("input file", "/abc/", "def", "/abc/def"),
            ("input abs file with root /", "/", "/", "/"),
            ("input dir with root /", "/", "def/", "/def/"),
            ("input file with root /", "/", "def", "/def"),
        ];

        for (name, root, input, expect) in cases {
            let actual = build_rooted_absolute_path(root, input);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_build_rel_path() {
        let cases = vec![
            ("input abs file", "/abc/", "/abc/def", "def"),
            ("input dir", "/abc/", "/abc/def/", "def/"),
            ("input file", "/abc/", "abc/def", "def"),
            ("input dir with root /", "/", "def/", "def/"),
            ("input file with root /", "/", "def", "def"),
        ];

        for (name, root, input, expect) in cases {
            let actual = build_relative_path(root, input);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_validate_path() {
        let cases = vec![
            ("input file with mode file", "abc", EntryMode::FILE, true),
            ("input file with mode dir", "abc", EntryMode::DIR, false),
            ("input dir with mode file", "abc/", EntryMode::FILE, false),
            ("input dir with mode dir", "abc/", EntryMode::DIR, true),
            ("root with mode dir", "/", EntryMode::DIR, true),
            (
                "input file with mode unknown",
                "abc",
                EntryMode::Unknown,
                false,
            ),
            (
                "input dir with mode unknown",
                "abc/",
                EntryMode::Unknown,
                false,
            ),
        ];

        for (name, path, mode, expect) in cases {
            let actual = validate_path(path, mode);
            assert_eq!(actual, expect, "{name}")
        }
    }

    #[test]
    fn test_build_tmp_path_of() {
        let cases = vec![
            ("a file path", "example.txt", "example.txt."),
            (
                "a file path in a directory",
                "folder/example.txt",
                "example.txt.",
            ),
        ];

        for (name, path, expect_starts_with) in cases {
            let actual = build_tmp_path_of(path);
            assert!(
                actual.starts_with(expect_starts_with),
                "{name}: got `{actual}`, but expect `{expect_starts_with}`"
            );
            assert_eq!(
                actual.len(),
                expect_starts_with.len() + 8, // See RANDOM_TMP_PATH_POSTFIX_SIZE
                "{name}: got `{actual}`, but expect `{expect_starts_with}`"
            )
        }
    }
}
