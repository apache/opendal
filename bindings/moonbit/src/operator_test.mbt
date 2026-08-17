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
test "memory: whole-object binary roundtrip" {
  let operator = @opendal.Operator::new("memory")
  defer operator.close()
  let payload : Bytes = [0x00, 0x01, 0x7f, 0x80, 0xff, 0x00, 0x42]
  operator.write("目录/embedded-nul.bin", payload)
  assert_eq(operator.read("目录/embedded-nul.bin"), payload)
  operator.write("empty.bin", Bytes::new(0))
  assert_eq(operator.read("empty.bin"), Bytes::new(0))
}

///|
test "memory: errors preserve kind, operation, and path" {
  try @opendal.Operator::new("missing-service") catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.Unsupported)
      assert_true(info.operation is @opendal.NewOperator)
      assert_eq(info.path, None)
    }
  } noraise {
    _ => fail("expected an unavailable service to raise")
  }
  let operator = @opendal.Operator::new("memory")
  defer operator.close()
  try operator.read("missing.bin") catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.NotFound)
      assert_true(info.operation is @opendal.Read)
      assert_eq(info.path, Some("missing.bin"))
    }
  } noraise {
    _ => fail("expected a missing read to raise")
  }
  try operator.write("invalid\u0000path", b"value") catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.InvalidArgument)
      assert_true(info.operation is @opendal.Write)
    }
  } noraise {
    _ => fail("expected an embedded NUL path to raise")
  }
  let invalid_utf16 = String::from_array([(0xd800).unsafe_to_char()])
  try operator.read(invalid_utf16) catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.InvalidArgument)
      assert_true(info.operation is @opendal.Read)
      assert_eq(info.path, Some(invalid_utf16))
    }
  } noraise {
    _ => fail("expected invalid UTF-16 to raise")
  }
}

///|
test "memory: close is idempotent and blocks later operations" {
  let operator = @opendal.Operator::new("memory")
  operator.close()
  operator.close()
  try operator.read("closed.bin") catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.ResourceClosed)
      assert_true(info.operation is @opendal.Read)
      assert_eq(info.path, Some("closed.bin"))
    }
  } noraise {
    _ => fail("expected a read after close to raise")
  }
  try operator.write("closed.bin", b"value") catch {
    @opendal.OpenDalError(info) => {
      assert_true(info.kind is @opendal.ResourceClosed)
      assert_true(info.operation is @opendal.Write)
      assert_eq(info.path, Some("closed.bin"))
    }
  } noraise {
    _ => fail("expected a write after close to raise")
  }
}
