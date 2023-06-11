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

use opendal::Operator;
use opendal::Result;
use opendal::services::Fs;

#[tokio::main]
async fn main() -> Result<()> {

    // Create fs backend builder.
    let mut builder = Fs::default();

    // set the root for fs, all operations will happen under this root.
    // note that the root must be absolute path.
    builder.root("/home/flash/projects/opendal-example");

    // create an `Operator` from `builder`, all file operations are initiated from it.
    let op: Operator = Operator::new(builder)?.finish();

    // if the 'root' path haven't been set, then the `file_path` below should be `/home/flash/projects/opendal-example/1.txt`.
    let file_path = "1.txt";

    // read the file and print its content.
    let read_file = op.read(file_path).await?;
    let content = String::from_utf8(read_file).unwrap();
    println!("{}", content);

    // write the file.
    op.write(file_path, "who are you").await?;

    // verify the file content after the write operator.
    let read_file = op.read(file_path).await?;
    let content = String::from_utf8(read_file).unwrap();
    println!("{}", content);

    // copy the file to `2.txt` and delete the old file.
    op.copy(file_path, "2.txt").await?;
    op.delete(file_path).await?;

    Ok(())
}