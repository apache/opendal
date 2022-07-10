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
use anyhow::{anyhow, Result};
use clap::{App, AppSettings, SubCommand};

pub fn main() -> Result<()> {
    let _ = cli("ocp").get_matches();
    println!("got ocp");
    Ok(())
}

pub(crate) fn cli(name: &str) -> App<'static> {
    let mut app = App::new(name)
        .version("0.10.0")
        .about("copy")
        .setting(AppSettings::DeriveDisplayOrder)
        .setting(AppSettings::SubcommandRequiredElseHelp);

    app
}
