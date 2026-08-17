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
fn create_unclosed_operator() -> Unit raise OpenDalError {
  let operator = Operator::new("memory")
  operator.write("finalized.bin", b"value")
}

///|
extern "C" fn native_live_operator_count() -> UInt = "moonbit_opendal_live_operator_count"

///|
test "memory: close and finalization release the native operator once" {
  let baseline = native_live_operator_count()
  let operator = Operator::new("memory")
  assert_eq(native_live_operator_count(), baseline + 1)
  operator.close()
  operator.close()
  assert_eq(native_live_operator_count(), baseline)
  create_unclosed_operator()
  assert_eq(native_live_operator_count(), baseline)
}

///|
#borrow(operator, path)
extern "C" fn native_operator_read_with_limit_for_test(
  operator : Operator,
  path : Bytes,
  max_read_bytes : UInt,
) -> NativeResult = "moonbit_opendal_operator_read_with_limit_for_test"

///|
test "memory: whole-object reads enforce their allocation limit" {
  let operator = Operator::new("memory")
  defer operator.close()
  let path = "limited.bin"
  operator.write(path, b"nine-byte")
  let result = native_operator_read_with_limit_for_test(
    operator,
    encode_native_text(path, Read, Some(path), "path"),
    8,
  )
  assert_true(native_result_status(result) != 0)
  let error = error_from_native_result(result, Read, Some(path))
  assert_true(error.kind() is BufferTooLarge)
}
