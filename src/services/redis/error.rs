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

use std::io::Error;

use anyhow::anyhow;
use redis::RedisError;

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;

pub(crate) fn new_async_connection_error(err: RedisError, op: Operation, path: &str) -> Error {
    new_other_object_error(op, path, anyhow!("getting async connection: {err:?}"))
}

pub(crate) fn new_serialize_metadata_error(
    err: bincode::Error,
    op: Operation,
    path: &str,
) -> Error {
    new_other_object_error(op, path, anyhow!("serialize metadata: {err:?}"))
}

pub(crate) fn new_deserialize_metadata_error(
    err: bincode::Error,
    op: Operation,
    path: &str,
) -> Error {
    new_other_object_error(op, path, anyhow!("deserialize metadata: {err:?}"))
}

pub(crate) fn new_exec_async_cmd_error(err: RedisError, op: Operation, path: &str) -> Error {
    new_other_object_error(op, path, anyhow!("executing async command: {err:?}"))
}
