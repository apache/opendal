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

mod generate;
mod release;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap()
}

fn workspace_dir() -> PathBuf {
    manifest_dir().join("..").canonicalize().unwrap()
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cmd {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate all services configs for opendal.
    Generate {
        #[arg(short, long)]
        language: String,
    },
    /// Update the version of all packages.
    UpdateVersion,
}

fn main() -> anyhow::Result<()> {
    logforth::stderr().apply();

    match Cmd::parse().command {
        Commands::Generate { language } => generate::run(&language),
        Commands::UpdateVersion => release::update_version(),
    }
}
