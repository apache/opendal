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

use crate::{Error, ErrorKind, Result};

/// Compiled Unix shell style glob pattern for path-component matching.
///
/// Services implementing native [`crate::Capability::list_with_glob`] and
/// [`crate::layers::SimulateLayer`] share this matcher to evaluate
/// [`crate::options::ListOptions::glob`] consistently and prune directories
/// during traversal.
#[derive(Debug)]
pub struct GlobPattern {
    segments: Vec<Segment>,
    directories_only: bool,
}

impl GlobPattern {
    /// Parse and compile a glob pattern.
    ///
    /// Returns [`ErrorKind::ConfigInvalid`] if the pattern is empty, contains
    /// malformed syntax, or uses disallowed literal components such as `.` or
    /// `..`.
    pub fn new(pattern: &str) -> Result<Self> {
        let mut segments = Vec::new();
        for segment in pattern.split('/').filter(|part| !part.is_empty()) {
            if segment == "**" {
                if !matches!(segments.last(), Some(Segment::Recursive)) {
                    segments.push(Segment::Recursive);
                }
                continue;
            }
            let matcher = SegmentMatcher::new(segment)?;
            if let Some(literal) = matcher.literal() {
                if matches!(literal.as_str(), "." | "..") || literal.contains('\0') {
                    return Err(invalid("invalid literal path component in glob pattern"));
                }
                segments.push(Segment::Literal(literal));
            } else {
                segments.push(Segment::Glob(matcher));
            }
        }
        if segments.is_empty() {
            return Err(invalid("glob pattern must contain a path component"));
        }
        Ok(Self {
            segments,
            directories_only: pattern.ends_with('/'),
        })
    }

    /// Yield leading literal directory components that can be entered directly
    /// without listing their parent directories.
    pub fn literal_directories(&self) -> impl Iterator<Item = &str> {
        let terminal_components = if matches!(self.segments.last(), Some(Segment::Recursive)) {
            // The directory before a trailing `**` can itself match.
            2
        } else {
            1
        };
        self.segments[..self.segments.len().saturating_sub(terminal_components)]
            .iter()
            .map_while(|segment| match segment {
                // Backslashes and drive prefixes can have path semantics on some services.
                Segment::Literal(value) if !value.contains(['\\', ':']) => Some(value.as_str()),
                _ => None,
            })
    }

    /// Return `true` if the pattern can match entries deeper than one directory level.
    pub fn spans_directories(&self) -> bool {
        self.segments.len() > 1 || matches!(self.segments[0], Segment::Recursive)
    }

    /// Whether a guided walk from a directory at `position` lists every directory below it.
    pub(crate) fn walks_whole_subtree(&self, position: usize) -> bool {
        self.segments[position..]
            .iter()
            .find(
                |segment| !matches!(segment, Segment::Glob(matcher) if matcher.matches_any_name()),
            )
            .is_some_and(|segment| matches!(segment, Segment::Recursive))
    }

    /// Advance every viable pattern position against a single path component.
    ///
    /// The returned positions are sorted and deduplicated. Start from `&[0]` at
    /// the base directory, or `&[n]` after skipping `n` leading literal
    /// directories returned by [`Self::literal_directories`].
    pub fn advance(&self, positions: &[usize], name: &str) -> Vec<usize> {
        let mut next = Vec::new();
        for &position in positions {
            match self.segments.get(position) {
                Some(Segment::Recursive) => {
                    next.push(position);
                    match self.segments.get(position + 1) {
                        Some(segment) if segment.matches(name) => next.push(position + 2),
                        None => next.push(position + 1),
                        _ => {}
                    }
                }
                Some(segment) if segment.matches(name) => next.push(position + 1),
                _ => {}
            }
        }
        next.sort_unstable();
        next.dedup();
        next
    }

    /// Return `true` if `positions` represent a complete match for an entry.
    ///
    /// Patterns with a trailing `/` require `is_dir` to be `true`.
    pub fn accepts(&self, positions: &[usize], is_dir: bool) -> bool {
        if self.directories_only && !is_dir {
            return false;
        }
        let end = self.segments.len();
        positions.last() == Some(&end)
            || (is_dir
                && matches!(self.segments.last(), Some(Segment::Recursive))
                && positions.last() == Some(&(end - 1)))
    }

    /// Return `true` if descending into a directory at `positions` can still
    /// match descendants.
    pub fn can_descend(&self, positions: &[usize]) -> bool {
        positions
            .first()
            .is_some_and(|&first| first < self.segments.len())
    }

    /// Return `true` if a relative path matches the pattern.
    pub fn matches(&self, path: &str, is_dir: bool) -> bool {
        let mut positions = vec![0];
        for component in path.trim_end_matches('/').split('/') {
            positions = self.advance(&positions, component);
            if positions.is_empty() {
                return false;
            }
        }
        self.accepts(&positions, is_dir)
    }
}

#[derive(Debug)]
enum Segment {
    Literal(String),
    Glob(SegmentMatcher),
    Recursive,
}

impl Segment {
    fn matches(&self, name: &str) -> bool {
        match self {
            Self::Literal(literal) => literal == name,
            Self::Glob(matcher) => matcher.matches(name),
            Self::Recursive => unreachable!("consecutive recursive wildcards are collapsed"),
        }
    }
}

#[derive(Debug)]
struct SegmentMatcher {
    tokens: Vec<Token>,
}

#[derive(Debug)]
enum Token {
    Literal(char),
    Any,
    Star,
    Class {
        ranges: Vec<(char, char)>,
        negated: bool,
    },
    Split(usize),
    Jump(usize),
}

#[derive(Default)]
struct Alternation {
    split: usize,
    exits: Vec<usize>,
}

impl SegmentMatcher {
    fn new(segment: &str) -> Result<Self> {
        let mut chars = segment.chars().peekable();
        let mut tokens = Vec::new();
        let mut alternations = Vec::<Alternation>::new();
        while let Some(ch) = chars.next() {
            let token = match ch {
                '*' => {
                    if chars.peek() == Some(&'*') {
                        return Err(invalid("`**` must occupy a whole glob path component"));
                    }
                    Token::Star
                }
                '?' => Token::Any,
                '[' => parse_class(&mut chars)?,
                '\\' => Token::Literal(escaped_char(&mut chars)?),
                '{' => {
                    alternations.push(Alternation {
                        split: tokens.len(),
                        ..Default::default()
                    });
                    Token::Split(0)
                }
                ',' if !alternations.is_empty() => {
                    let group = alternations.last_mut().unwrap();
                    group.exits.push(tokens.len());
                    tokens.push(Token::Jump(0));
                    let next_branch = tokens.len();
                    tokens[group.split] = Token::Split(next_branch);
                    group.split = next_branch;
                    Token::Split(0)
                }
                '}' if !alternations.is_empty() => {
                    let group = alternations.pop().unwrap();
                    tokens[group.split] = Token::Jump(group.split + 1);
                    let end = tokens.len();
                    for exit in group.exits {
                        tokens[exit] = Token::Jump(end);
                    }
                    continue;
                }
                _ => Token::Literal(ch),
            };
            tokens.push(token);
        }
        if !alternations.is_empty() {
            return Err(invalid("unterminated alternation in glob pattern"));
        }
        Ok(Self { tokens })
    }

    fn matches_any_name(&self) -> bool {
        matches!(self.tokens.as_slice(), [Token::Star])
    }

    fn literal(&self) -> Option<String> {
        self.tokens
            .iter()
            .map(|token| match token {
                Token::Literal(ch) => Some(*ch),
                _ => None,
            })
            .collect()
    }

    fn matches(&self, name: &str) -> bool {
        if name.contains('/') {
            return false;
        }
        let mut current = vec![false; self.tokens.len() + 1];
        let mut next = current.clone();
        current[0] = true;
        for ch in name.chars().map(Some).chain(std::iter::once(None)) {
            // Branch targets only point forward. A single scan resolves all
            // transitions that consume no characters before advancing the input.
            for (index, token) in self.tokens.iter().enumerate() {
                if !current[index] {
                    continue;
                }
                match token {
                    Token::Star => {
                        current[index + 1] = true;
                        next[index] |= ch.is_some();
                    }
                    Token::Split(branch) => {
                        current[index + 1] = true;
                        current[*branch] = true;
                    }
                    Token::Jump(target) => current[*target] = true,
                    Token::Any => next[index + 1] |= ch.is_some(),
                    Token::Literal(literal) => next[index + 1] |= ch == Some(*literal),
                    Token::Class { ranges, negated } => {
                        next[index + 1] |= ch.is_some_and(|ch| {
                            ranges.iter().any(|(lo, hi)| *lo <= ch && ch <= *hi) != *negated
                        });
                    }
                }
            }
            if ch.is_none() {
                break;
            }
            std::mem::swap(&mut current, &mut next);
            next.fill(false);
        }
        current[self.tokens.len()]
    }
}

fn escaped_char(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<char> {
    chars
        .next()
        .ok_or_else(|| invalid("trailing backslash in glob path component"))
}

fn parse_class(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<Token> {
    let negated = chars.next_if(|ch| matches!(ch, '!' | '^')).is_some();
    let mut ranges = Vec::new();
    while let Some(ch) = chars.next() {
        if ch == ']' && !ranges.is_empty() {
            return Ok(Token::Class { ranges, negated });
        }
        let start = if ch == '\\' { escaped_char(chars)? } else { ch };
        if chars.next_if_eq(&'-').is_none() {
            ranges.push((start, start));
            continue;
        }
        let end = match chars.peek() {
            Some(']') => {
                ranges.push((start, start));
                ranges.push(('-', '-'));
                continue;
            }
            Some('\\') => {
                chars.next();
                escaped_char(chars)?
            }
            _ => escaped_char(chars)?,
        };
        if end < start {
            return Err(invalid("reversed character range in glob pattern"));
        }
        ranges.push((start, end));
    }
    Err(invalid("unterminated character class in glob pattern"))
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_one_component_by_unicode_scalar() {
        for (pattern, name, expected) in [
            ("*.jpg", "a.jpg", true),
            ("*.jpg", "dir/a.jpg", false),
            ("?.jpg", "猫.jpg", true),
            ("?.jpg", "猫犬.jpg", false),
            ("?.jpg", "/.jpg", false),
            ("[à-é]", "é", true),
            ("[!a-z]", "é", true),
            ("[!a-z]", "a", false),
            ("[]a]", "]", true),
            ("[**]", "*", true),
            (r"[a\-z]", "-", true),
            (r"[a\-z]", "m", false),
            (r"\*\?\[\\", "*?[\\", true),
            (r"foo\**", "foo*猫", true),
            ("*.{jpg,{png,gif}}", "猫.gif", true),
            ("{,é}.txt", ".txt", true),
            ("{a,b}{1,2}", "b2", true),
            ("{a,b}{1,2}", "a", false),
        ] {
            assert_eq!(
                GlobPattern::new(pattern).unwrap().matches(name, false),
                expected,
                "{pattern:?} against {name:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_patterns() {
        for pattern in [
            "",
            "/",
            "a**b",
            "a/***/b",
            "file\\",
            "[",
            "[]",
            "[z-a]",
            "{one,two",
            "../*.jpg",
            r"\../*.jpg",
        ] {
            assert_eq!(
                GlobPattern::new(pattern).unwrap_err().kind(),
                ErrorKind::ConfigInvalid,
                "{pattern:?}"
            );
        }
    }

    #[test]
    fn only_leading_literal_directories_can_be_skipped() {
        let pattern = GlobPattern::new(r"a/\[b\]/**/*.json").unwrap();
        assert_eq!(
            pattern.literal_directories().collect::<Vec<_>>(),
            ["a", "[b]"]
        );
        assert!(pattern.matches("a/[b]/x.json", false));
        assert!(pattern.matches("a/[b]/c/x.json", false));
        assert!(!pattern.matches("a/c/x.json", false));
    }
}
