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

//! Internal Unix shell style glob matcher used by `GlobLayer` to provide
//! `list_with_glob` semantics for services that lack native support.
//!
//! Exposes [`SegmentMatcher`] (compiles a single segment with no `/`) plus
//! [`compile_segments`] and the [`Segment`] enum that classifies each part of
//! a pattern. These are the building blocks consumed by
//! [`crate::layers::GlobLayer`]'s guided traversal.
//!
//! Kept dependency-free on purpose; supports `*`, `?`, `**`, character classes
//! (`[a-z]`, `[!abc]`) and alternation (`{a,b}`).

use crate::*;

/// Classification of a single pattern segment used by guided traversal.
#[derive(Debug, Clone)]
pub(crate) enum Segment {
    /// A literal path piece — append directly to the path without listing.
    ///
    /// At the leading edge of a pattern this may contain `/` (consecutive
    /// literal segments are folded together by `compile_segments` into a
    /// single `Literal("a/b/c")`). After any `Glob` or `DoubleStar` has
    /// appeared, every `Literal` corresponds to a single path component
    /// and contains no `/`.
    Literal(String),
    /// Single-segment glob (`*.jpg`, `[ab].txt`, `{x,y}.csv`). Matched by
    /// listing the parent and filtering child names.
    Glob(SegmentMatcher),
    /// `**` — match zero or more path components.
    DoubleStar,
}

/// Compiled single-segment glob. The input must not contain `/`.
#[derive(Debug, Clone)]
pub(crate) struct SegmentMatcher {
    nodes: Vec<Node>,
}

impl SegmentMatcher {
    pub(crate) fn new(segment: &str) -> Result<Self> {
        debug_assert!(
            !segment.contains('/'),
            "SegmentMatcher::new received a segment containing '/': {segment:?}",
        );
        // `**` is only meaningful as a whole segment. Embedded forms like
        // `a**b` are rejected by fsspec; reject them here for parity with the
        // documented surface so users don't get silently surprising matches.
        if segment != "**" && segment.contains("**") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("glob segment {segment:?} contains `**` outside a whole segment"),
            ));
        }
        let mut chars = segment.chars().peekable();
        let nodes = parse_nodes(&mut chars, false)?;
        Ok(Self { nodes })
    }

    /// True if `name` (no `/`) matches the compiled segment.
    pub(crate) fn matches(&self, name: &str) -> bool {
        match_nodes_at(&self.nodes, 0, name.as_bytes(), 0)
    }
}

/// Compile a pattern into the per-segment representation used by guided
/// traversal. Returns the segments in order.
///
/// Two collapses happen at compile time to reduce work at traversal time:
///
/// - **Consecutive `**`**: `**/**` ≡ `**`. Both match "zero or more
///   components". Collapsing avoids redundant list calls.
/// - **Leading literal prefix**: consecutive literal segments at the very
///   start of the pattern (before any `Glob` or `DoubleStar`) are folded
///   into a single `Literal("a/b/c")`. This skips a frame-loop iteration
///   per segment when seeding the traversal, which is the most common
///   case (e.g. `a/b/c/**/*.json`). Literals following a wildcard segment
///   are kept separate so that downstream entry handlers — which compare
///   the next segment against a single basename — can match them
///   unambiguously.
pub(crate) fn compile_segments(pattern: &str) -> Result<Vec<Segment>> {
    let mut out: Vec<Segment> = Vec::new();
    // Only fold while we are still in the leading literal prefix region.
    let mut in_leading_prefix = true;
    for seg in split_segments(pattern) {
        if seg == "**" {
            in_leading_prefix = false;
            // Skip if previous segment is already `**` — `**/**` ≡ `**`.
            if matches!(out.last(), Some(Segment::DoubleStar)) {
                continue;
            }
            out.push(Segment::DoubleStar);
        } else if has_meta(seg) {
            in_leading_prefix = false;
            out.push(Segment::Glob(SegmentMatcher::new(seg)?));
        } else if in_leading_prefix {
            if let Some(Segment::Literal(prev)) = out.last_mut() {
                prev.push('/');
                prev.push_str(seg);
            } else {
                out.push(Segment::Literal(seg.to_string()));
            }
        } else {
            out.push(Segment::Literal(seg.to_string()));
        }
    }
    Ok(out)
}

/// Split a glob pattern into `/`-delimited segments, dropping empty parts.
pub(crate) fn split_segments(pattern: &str) -> Vec<&str> {
    pattern.split('/').filter(|s| !s.is_empty()).collect()
}

/// True when the segment contains any glob metacharacter.
pub(crate) fn has_meta(segment: &str) -> bool {
    segment
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']' | '{' | '}'))
}

#[derive(Debug, Clone)]
enum Node {
    Literal(char),
    AnyChar,
    AnyRun,
    AnyRecursive,
    Class(Vec<(char, char)>, bool /* negated */),
    Alt(Vec<Vec<Node>>),
    Separator,
}

fn parse_nodes(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    in_alt: bool,
) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    while let Some(&c) = chars.peek() {
        match c {
            ',' | '}' if in_alt => break,
            '*' => {
                chars.next();
                if chars.peek() == Some(&'*') {
                    chars.next();
                    // `**/` should match zero or more path components; consume
                    // the trailing separator so the recursive wildcard owns it.
                    if chars.peek() == Some(&'/') {
                        chars.next();
                    }
                    nodes.push(Node::AnyRecursive);
                } else {
                    nodes.push(Node::AnyRun);
                }
            }
            '?' => {
                chars.next();
                nodes.push(Node::AnyChar);
            }
            '/' => {
                chars.next();
                nodes.push(Node::Separator);
            }
            '[' => {
                chars.next();
                nodes.push(parse_class(chars)?);
            }
            '{' => {
                chars.next();
                nodes.push(parse_alt(chars)?);
            }
            '\\' => {
                chars.next();
                match chars.next() {
                    Some(escaped) => nodes.push(Node::Literal(escaped)),
                    None => {
                        return Err(Error::new(
                            ErrorKind::ConfigInvalid,
                            "trailing `\\` in glob pattern",
                        ));
                    }
                }
            }
            _ => {
                chars.next();
                nodes.push(Node::Literal(c));
            }
        }
    }
    Ok(nodes)
}

fn parse_class(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<Node> {
    let mut ranges: Vec<(char, char)> = Vec::new();
    let mut negated = false;
    if chars.peek() == Some(&'!') || chars.peek() == Some(&'^') {
        negated = true;
        chars.next();
    }
    while let Some(&c) = chars.peek() {
        if c == ']' {
            chars.next();
            return Ok(Node::Class(ranges, negated));
        }
        chars.next();
        let start = c;
        if chars.peek() == Some(&'-') {
            chars.next();
            if let Some(&end) = chars.peek() {
                if end == ']' {
                    ranges.push((start, start));
                    ranges.push(('-', '-'));
                } else {
                    chars.next();
                    ranges.push((start, end));
                }
            }
        } else {
            ranges.push((start, start));
        }
    }
    Err(Error::new(
        ErrorKind::ConfigInvalid,
        "unterminated character class in glob pattern",
    ))
}

fn parse_alt(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<Node> {
    let mut alts = Vec::new();
    loop {
        let branch = parse_nodes(chars, true)?;
        alts.push(branch);
        match chars.next() {
            Some(',') => continue,
            Some('}') => return Ok(Node::Alt(alts)),
            _ => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "unterminated alternation in glob pattern",
                ));
            }
        }
    }
}

fn match_nodes_at(nodes: &[Node], ni: usize, text: &[u8], ti: usize) -> bool {
    if ni == nodes.len() {
        return ti == text.len();
    }

    match &nodes[ni] {
        Node::Literal(c) => {
            let mut buf = [0u8; 4];
            let s = c.encode_utf8(&mut buf).as_bytes();
            if text[ti..].starts_with(s) {
                match_nodes_at(nodes, ni + 1, text, ti + s.len())
            } else {
                false
            }
        }
        Node::Separator => {
            if text.get(ti) == Some(&b'/') {
                match_nodes_at(nodes, ni + 1, text, ti + 1)
            } else {
                false
            }
        }
        Node::AnyChar => {
            if ti < text.len() && text[ti] != b'/' {
                match_nodes_at(nodes, ni + 1, text, ti + 1)
            } else {
                false
            }
        }
        Node::AnyRun => {
            let mut end = ti;
            while end < text.len() && text[end] != b'/' {
                end += 1;
            }
            let mut k = end;
            loop {
                if match_nodes_at(nodes, ni + 1, text, k) {
                    return true;
                }
                if k == ti {
                    return false;
                }
                k -= 1;
            }
        }
        Node::AnyRecursive => {
            let mut k = ti;
            loop {
                if match_nodes_at(nodes, ni + 1, text, k) {
                    return true;
                }
                if k == text.len() {
                    return false;
                }
                k += 1;
            }
        }
        Node::Class(ranges, negated) => {
            if ti >= text.len() {
                return false;
            }
            let ch = text[ti] as char;
            if ch == '/' {
                return false;
            }
            let mut hit = false;
            for (lo, hi) in ranges {
                if ch >= *lo && ch <= *hi {
                    hit = true;
                    break;
                }
            }
            if hit ^ *negated {
                match_nodes_at(nodes, ni + 1, text, ti + 1)
            } else {
                false
            }
        }
        Node::Alt(branches) => {
            for branch in branches {
                let mut combined = Vec::with_capacity(branch.len() + nodes.len() - ni - 1);
                combined.extend_from_slice(branch);
                combined.extend_from_slice(&nodes[ni + 1..]);
                if match_nodes_at(&combined, 0, text, ti) {
                    return true;
                }
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pat: &str, name: &str) -> bool {
        SegmentMatcher::new(pat).unwrap().matches(name)
    }

    #[test]
    fn star_does_not_cross_separator() {
        assert!(m("*.jpg", "a.jpg"));
        assert!(!m("*.jpg", "dir/a.jpg"));
    }

    #[test]
    fn question_mark_single_char() {
        assert!(m("?.txt", "a.txt"));
        assert!(!m("?.txt", "ab.txt"));
        assert!(!m("?.txt", "/.txt"));
    }

    #[test]
    fn character_class() {
        assert!(m("[abc].rs", "a.rs"));
        assert!(m("[a-z].rs", "k.rs"));
        assert!(!m("[!a-z].rs", "k.rs"));
        assert!(m("[!a-z].rs", "1.rs"));
    }

    #[test]
    fn alternation() {
        assert!(m("*.{jpg,png}", "a.jpg"));
        assert!(m("*.{jpg,png}", "a.png"));
        assert!(!m("*.{jpg,png}", "a.gif"));
    }

    #[test]
    fn segment_helpers() {
        let segs = split_segments("media/2024/**/*.jpg");
        assert_eq!(segs, vec!["media", "2024", "**", "*.jpg"]);
        assert!(!has_meta("media"));
        assert!(has_meta("*.jpg"));
        assert!(has_meta("**"));
    }

    #[test]
    fn segment_matcher_basic() {
        let m = SegmentMatcher::new("*.jpg").unwrap();
        assert!(m.matches("a.jpg"));
        assert!(!m.matches("a.png"));
        assert!(!m.matches("dir/a.jpg")); // segment matcher never crosses '/'

        let m = SegmentMatcher::new("[ab].*").unwrap();
        assert!(m.matches("a.jpg"));
        assert!(m.matches("b.png"));
        assert!(!m.matches("c.jpg"));

        let m = SegmentMatcher::new("?.txt").unwrap();
        assert!(m.matches("a.txt"));
        assert!(!m.matches("ab.txt"));

        let m = SegmentMatcher::new("{foo,bar}.csv").unwrap();
        assert!(m.matches("foo.csv"));
        assert!(m.matches("bar.csv"));
        assert!(!m.matches("baz.csv"));
    }

    #[test]
    fn compile_segments_classifies() {
        let segs = compile_segments("media/2024/**/*.jpg").unwrap();
        assert_eq!(segs.len(), 3);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "media/2024"));
        assert!(matches!(segs[1], Segment::DoubleStar));
        assert!(matches!(&segs[2], Segment::Glob(_)));
    }

    #[test]
    fn compile_segments_collapses_consecutive_double_stars() {
        let segs = compile_segments("**/**/x.txt").unwrap();
        assert_eq!(segs.len(), 2);
        assert!(matches!(segs[0], Segment::DoubleStar));
        assert!(matches!(&segs[1], Segment::Literal(s) if s == "x.txt"));

        let segs = compile_segments("a/**/**/**/b").unwrap();
        assert_eq!(segs.len(), 3);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "a"));
        assert!(matches!(segs[1], Segment::DoubleStar));
        assert!(matches!(&segs[2], Segment::Literal(s) if s == "b"));
    }

    #[test]
    fn compile_segments_folds_leading_literal_prefix() {
        // Leading literal run folds into one segment.
        let segs = compile_segments("a/b/c/**/*.jpg").unwrap();
        assert_eq!(segs.len(), 3);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "a/b/c"));
        assert!(matches!(segs[1], Segment::DoubleStar));
        assert!(matches!(&segs[2], Segment::Glob(_)));

        // Pure literal pattern folds end-to-end.
        let segs = compile_segments("a/b/c.txt").unwrap();
        assert_eq!(segs.len(), 1);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "a/b/c.txt"));

        // Literals after a wildcard segment stay one-component-per-Literal
        // so downstream basename matching works.
        let segs = compile_segments("a/*/b/c").unwrap();
        assert_eq!(segs.len(), 4);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "a"));
        assert!(matches!(&segs[1], Segment::Glob(_)));
        assert!(matches!(&segs[2], Segment::Literal(s) if s == "b"));
        assert!(matches!(&segs[3], Segment::Literal(s) if s == "c"));

        // Literals after `**` likewise stay one-component-per-Literal.
        let segs = compile_segments("a/**/b/c").unwrap();
        assert_eq!(segs.len(), 4);
        assert!(matches!(&segs[0], Segment::Literal(s) if s == "a"));
        assert!(matches!(segs[1], Segment::DoubleStar));
        assert!(matches!(&segs[2], Segment::Literal(s) if s == "b"));
        assert!(matches!(&segs[3], Segment::Literal(s) if s == "c"));
    }

    #[test]
    fn segment_matcher_rejects_double_star_in_segment() {
        assert!(SegmentMatcher::new("a**b").is_err());
        assert!(SegmentMatcher::new("**suffix").is_err());
        // Bare `**` segment is fine.
        assert!(SegmentMatcher::new("**").is_ok());
    }

    #[test]
    fn parse_rejects_trailing_backslash() {
        assert!(SegmentMatcher::new("foo\\").is_err());
    }
}
