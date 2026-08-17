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
pub fn OpenDalError::info(self : OpenDalError) -> ErrorInfo {
  match self {
    OpenDalError(info) => info
  }
}

///|
pub fn OpenDalError::kind(self : OpenDalError) -> ErrorKind {
  self.info().kind
}

///|
pub impl Show for OpenDalError with fn output(self, logger) {
  let info = self.info()
  logger <+
    "OpenDAL \{Repr(info.kind)} during \{Repr(info.operation)}: \{info.message}"
}

///|
fn error_kind_from_code(code : Int) -> ErrorKind {
  match code {
    0 => Unexpected
    1 => Unsupported
    2 => ConfigInvalid
    3 => NotFound
    4 => PermissionDenied
    5 => IsADirectory
    6 => NotADirectory
    7 => AlreadyExists
    8 => RateLimited
    9 => IsSameFile
    10 => ConditionNotMatch
    11 => RangeNotSatisfied
    0x1001 => ResourceClosed
    0x1002 => BufferTooLarge
    0x1003 => InvalidArgument
    code => Unknown(code)
  }
}

///|
fn encode_native_text(
  text : StringView,
  operation : Operation,
  path : String?,
  label : String,
) -> Bytes raise OpenDalError {
  let length = text.length()
  for index = 0; index < length; {
    let unit = text.unsafe_get(index)
    if unit.is_leading_surrogate() {
      if index + 1 >= length ||
        !text.unsafe_get(index + 1).is_trailing_surrogate() {
        raise OpenDalError({
          kind: InvalidArgument,
          message: "\{label} contains invalid UTF-16",
          operation,
          path,
        })
      }
      continue index + 2
    } else if unit.is_trailing_surrogate() {
      raise OpenDalError({
        kind: InvalidArgument,
        message: "\{label} contains invalid UTF-16",
        operation,
        path,
      })
    } else {
      continue index + 1
    }
  }
  @utf8.encode(text)
}

///|
fn error_from_native_result(
  result : NativeResult,
  operation : Operation,
  path : String?,
) -> OpenDalError {
  let kind = error_kind_from_code(native_result_error_kind(result))
  let message = @utf8.decode_lossy(native_result_error_message(result))
  native_result_release(result)
  OpenDalError({ kind, message, operation, path })
}
