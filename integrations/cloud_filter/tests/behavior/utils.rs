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

use std::fmt::Display;

use anyhow::Context;

pub fn file_length(path: impl Display) -> anyhow::Result<usize> {
    let len = powershell_script::run(&format!("(Get-Item \"{path}\" -Force).Length"))
        .context("run powershell")?
        .stdout()
        .unwrap_or_default()
        .trim()
        .parse()
        .context("parse length")?;

    Ok(len)
}

pub fn file_content(path: impl Display) -> anyhow::Result<String> {
    let content = powershell_script::run(&format!("Get-Content \"{path}\""))
        .context("run powershell")?
        .stdout()
        .unwrap_or_default()
        .replace("\r\n", "\n"); // powershell returns CRLF, but the fixtures use LF
    Ok(content)
}

pub fn list(path: impl Display, option: impl Display) -> anyhow::Result<Vec<String>> {
    let entries = powershell_script::run(&format!("(Get-ChildItem \"{path}\" -{option}).Name"))
        .context("run powershell")?
        .stdout()
        .unwrap_or_default()
        .lines()
        .map(Into::into)
        .collect();
    Ok(entries)
}
