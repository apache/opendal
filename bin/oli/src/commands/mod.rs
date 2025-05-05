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

//! Provides the implementation of each command.

pub mod bench;
pub mod cat;
pub mod cp;
pub mod ls;
pub mod mv;
pub mod rm;
pub mod stat;

#[derive(Debug, clap::Subcommand)]
pub enum OliSubcommand {
    Bench(bench::BenchCmd),
    Cat(cat::CatCmd),
    Cp(cp::CopyCmd),
    Ls(ls::LsCmd),
    Rm(rm::RmCmd),
    Stat(stat::StatCmd),
    Mv(mv::MoveCmd),
}

impl OliSubcommand {
    pub fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Bench(cmd) => cmd.run(),
            Self::Cat(cmd) => cmd.run(),
            Self::Cp(cmd) => cmd.run(),
            Self::Ls(cmd) => cmd.run(),
            Self::Rm(cmd) => cmd.run(),
            Self::Stat(cmd) => cmd.run(),
            Self::Mv(cmd) => cmd.run(),
        }
    }
}
