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
/// Construct an operator for a service scheme. This experimental phase builds
/// OpenDAL with only the memory service available.
pub fn Operator::new(scheme : StringView) -> Operator raise OpenDalError {
  let result = native_operator_new(
    encode_native_text(scheme, NewOperator, None, "service scheme"),
  )
  check_native_result(result, NewOperator, None)
  native_result_take_operator(result)
}

///|
/// Release this operator early. Calling `close` more than once is harmless.
pub fn Operator::close(self : Operator) -> Unit {
  native_operator_close(self)
}

///|
/// Read an entire object into an owned `Bytes` value.
pub fn Operator::read(
  self : Operator,
  path : StringView,
) -> Bytes raise OpenDalError {
  let path = path.to_owned()
  let result = native_operator_read(
    self,
    encode_native_text(path, Read, Some(path), "path"),
  )
  check_native_result(result, Read, Some(path))
  native_result_take_bytes(result)
}

///|
/// Replace an object with the complete contents of `data`.
pub fn Operator::write(
  self : Operator,
  path : StringView,
  data : BytesView,
) -> Unit raise OpenDalError {
  let path = path.to_owned()
  let result = native_operator_write(
    self,
    encode_native_text(path, Write, Some(path), "path"),
    data.to_owned(),
  )
  check_native_result(result, Write, Some(path))
}
