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

use opendal::services::Memory;
use opendal::Operator;
use opendal::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let op = Operator::new(Memory::default())?.finish();

    write_data(op.clone()).await?;
    read_data(op.clone()).await?;
    Ok(())
}

async fn write_data(op: Operator) -> Result<()> {
    op.write("test", "Hello, World!").await?;

    Ok(())
}

async fn read_data(op: Operator) -> Result<()> {
    let bs = op.read("test").await?;
    println!("data: {}", String::from_utf8_lossy(&bs));

    Ok(())
}
