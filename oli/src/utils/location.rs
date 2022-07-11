// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use opendal::Operator;

/// Parse `s3://abc/def` into `name` and `location`.
pub fn parse_location(s: &str) -> Result<(&str, &str)> {
    if !s.contains("://") {
        return Ok(("fs", s));
    }

    let s = s.splitn(2, "://").collect::<Vec<_>>();
    debug_assert!(s.len() == 2);
    Ok((s[0], s[1]))
}

/// If name is a valid scheme, we will load from env directly.
/// Or, we will try to get env from `OLI_PROFILE_{NAME}_XXX`.
///
/// Especially, the type is specified by `OLI_PROFILE_{NAME}_TYPE`
#[allow(dead_code)]
pub fn parse_profile(_: &str) -> Result<Operator> {
    todo!()
}

#[cfg(test)]
mod tests {
    use crate::utils::location::parse_location;

    #[test]
    fn test_parse_location() {
        let cases = vec![
            ("normal", "fs:///tmp", ("fs", "/tmp")),
            ("s3", "s3://test/path/to/file", ("s3", "test/path/to/file")),
            ("no scheme", "local", ("fs", "local")),
        ];

        for (name, input, expected) in cases {
            let actual = parse_location(input).expect("must succeed");

            assert_eq!(expected, actual, "{name}")
        }
    }
}
