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

//! Commands provides the implementation of each commands.
//!
//! Each submodule represents a single command, and should export 2 functions respectively.
//! The signature of those should be like the following:
//!
//! ```ignore
//! pub async fn main(args: &ArgMatches) -> Result<()> {
//!     // the main logic
//! }
//!
//! // cli is used to customize the command, like setting the arguments.
//! // As each command can be invoked like a separate binary,
//! // we will pass a command with different name to get the complete command.
//! pub fn cli(cmd: Command) -> Command {
//!    // set the arguments, help message, etc.
//! }
//! ```

pub mod cat;
pub mod cli;
pub mod cp;
pub mod ls;
