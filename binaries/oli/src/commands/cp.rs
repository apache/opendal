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

use anyhow::anyhow;
use anyhow::Result;
use clap::Arg;
use clap::ArgMatches;
use clap::Command;

use crate::utils::parse_location;

pub async fn main(args: Option<ArgMatches>) -> Result<()> {
    let args = args.unwrap_or_else(|| cli("ocp").get_matches());

    let src = args
        .get_one::<String>("source")
        .ok_or_else(|| anyhow!("missing source"))?;
    let (src_op, src_path) = parse_location(src)?;

    let dst = args
        .get_one::<String>("destination")
        .ok_or_else(|| anyhow!("missing target"))?;
    let (dst_op, dst_path) = parse_location(dst)?;
    let mut dst_w = dst_op.writer(dst_path).await?;

    let reader = src_op.reader(src_path).await?;
    let buf_reader = futures::io::BufReader::with_capacity(8 * 1024 * 1024, reader);
    futures::io::copy_buf(buf_reader, &mut dst_w).await?;
    Ok(())
}

pub(crate) fn cli(name: &str) -> Command {
    Command::new(name.to_string())
        .version("0.10.0")
        .about("copy")
        .arg(Arg::new("source").required(true))
        .arg(Arg::new("destination").required(true))
}
