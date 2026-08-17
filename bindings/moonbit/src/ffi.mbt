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
/// A call-scoped result whose finalizer releases untaken C resources.
priv type NativeResult

///|
#borrow(scheme)
extern "C" fn native_operator_new(scheme : Bytes) -> NativeResult = "moonbit_opendal_operator_new"

///|
#borrow(operator)
extern "C" fn native_operator_close(operator : Operator) -> Unit = "moonbit_opendal_operator_close"

///|
#borrow(operator, path)
extern "C" fn native_operator_read(
  operator : Operator,
  path : Bytes,
) -> NativeResult = "moonbit_opendal_operator_read"

///|
#borrow(operator, path, data)
extern "C" fn native_operator_write(
  operator : Operator,
  path : Bytes,
  data : Bytes,
) -> NativeResult = "moonbit_opendal_operator_write"

///|
#borrow(result)
extern "C" fn native_result_status(result : NativeResult) -> Int = "moonbit_opendal_result_status"

///|
#borrow(result)
extern "C" fn native_result_error_kind(result : NativeResult) -> Int = "moonbit_opendal_result_error_kind"

///|
#borrow(result)
extern "C" fn native_result_error_message(result : NativeResult) -> Bytes = "moonbit_opendal_result_error_message"

///|
#borrow(result)
extern "C" fn native_result_take_operator(result : NativeResult) -> Operator = "moonbit_opendal_result_take_operator"

///|
#borrow(result)
extern "C" fn native_result_take_bytes(result : NativeResult) -> Bytes = "moonbit_opendal_result_take_bytes"

///|
#borrow(result)
extern "C" fn native_result_release(result : NativeResult) -> Unit = "moonbit_opendal_result_release"
