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

///|
/// A native OpenDAL operator. The binding releases the underlying resource
/// when `close` is called or when this value becomes unreachable.
pub type Operator

///|
/// Stable error categories exposed by the MoonBit binding.
pub enum ErrorKind {
  Unexpected
  Unsupported
  ConfigInvalid
  NotFound
  PermissionDenied
  IsADirectory
  NotADirectory
  AlreadyExists
  RateLimited
  IsSameFile
  ConditionNotMatch
  RangeNotSatisfied
  ResourceClosed
  BufferTooLarge
  InvalidArgument
  Unknown(Int)
} derive(Eq, @debug.Debug)

///|
/// The operation that failed.
pub enum Operation {
  NewOperator
  Read
  Write
} derive(Eq, @debug.Debug)

///|
/// Structured context attached to an OpenDAL failure.
pub struct ErrorInfo {
  kind : ErrorKind
  message : String
  operation : Operation
  path : String?
} derive(Eq, @debug.Debug)

///|
/// The checked error raised by fallible OpenDAL operations.
pub suberror OpenDalError {
  OpenDalError(ErrorInfo)
} derive(Eq, @debug.Debug)
