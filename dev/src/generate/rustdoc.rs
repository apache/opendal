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

/// Rewrite the contents of code spans on one doc line.
///
/// Normalize ``[`name`]`` and ``[`name`](target)`` to plain code spans before
/// rewriting them so the callback only sees the visible text. Returning
/// `None` keeps the original span contents.
pub(super) fn rewrite_code_spans(line: &str, rewrite: impl Fn(&str) -> Option<String>) -> String {
    let line = strip_rustdoc_links(line);
    let mut out = String::with_capacity(line.len());
    let mut rest = line.as_str();
    while let Some(start) = rest.find('`') {
        let Some((code, tail)) = rest[start + 1..].split_once('`') else {
            break;
        };
        out.push_str(&rest[..=start]);
        out.push_str(rewrite(code).as_deref().unwrap_or(code));
        out.push('`');
        rest = tail;
    }
    out.push_str(rest);
    out
}

fn strip_rustdoc_links(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("[`") {
        let span_start = start + 1;
        let Some(end) = rest[span_start..].find("`]") else {
            break;
        };
        let span_end = span_start + end + 1;
        out.push_str(&rest[..start]);
        out.push_str(&rest[span_start..span_end]);
        rest = &rest[span_end + 1..];
        if let Some(target) = rest.strip_prefix('(')
            && let Some((_, tail)) = target.split_once(')')
        {
            rest = tail;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::rewrite_code_spans;

    #[test]
    fn test_rewrite_code_spans_preserves_incomplete_markup() {
        let rewrite = |code: &str| (code == "start_after").then(|| "startAfter".to_string());
        for (line, expected) in [
            (
                "See [`start_after`](crate::ListOptions::start_after",
                "See `startAfter`(crate::ListOptions::start_after",
            ),
            ("See [`start_after`", "See [`startAfter`"),
            (
                "Use `start_after` or `start_after",
                "Use `startAfter` or `start_after",
            ),
        ] {
            assert_eq!(rewrite_code_spans(line, rewrite), expected, "{line}");
        }
    }
}
