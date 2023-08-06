(*
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License")you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*)

open Opendal
open OUnit2

let test_check_result = function
  | Ok data -> data
  | Error err -> assert_failure err

let new_test_block_operator test_ctxt : blocking_operator =
  let cfgs = [ ("root", bracket_tmpdir test_ctxt) ] in
  let scheme = Blocking_operator.Scheme Scheme.Fs in
  test_check_result (Blocking_operator.new_operator scheme cfgs)

let new_test_block_operator_str test_ctxt : blocking_operator =
  let cfgs = [ ("root", bracket_tmpdir test_ctxt) ] in
  let scheme = Blocking_operator.SchemeStr "fs" in
  test_check_result (Blocking_operator.new_operator scheme cfgs)

let test_new_block_operator _ =
  ignore new_test_block_operator;
  ignore new_test_block_operator_str

let test_create_dir_and_remove_all test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore (test_check_result (Blocking_operator.create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (Blocking_operator.write bo "/testdir/foo" (Bytes.of_string "bar")));
  ignore
    (test_check_result
       (Blocking_operator.write bo "/testdir/bar" (Bytes.of_string "foo")));
  ignore (test_check_result (Blocking_operator.remove_all bo "/testdir/"))

let test_block_write_and_read test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Blocking_operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let data = test_check_result (Blocking_operator.read bo "tempfile") in
  assert_equal "helloworld"
    (data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

let test_copy_and_read test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  let data = "helloworld" in
  ignore
    (test_check_result
       (Blocking_operator.write bo "foo" (Bytes.of_string data)));
  ignore (test_check_result (Blocking_operator.copy bo "foo" "bar"));
  let got_res = test_check_result (Blocking_operator.read bo "bar") in
  assert_equal data (got_res |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

let suite =
  "suite"
  >::: [
         "test_new_block_operator" >:: test_new_block_operator;
         "test_create_dir_and_remove_all" >:: test_create_dir_and_remove_all;
         "test_block_write_and_read" >:: test_block_write_and_read;
         "test_copy_and_read" >:: test_copy_and_read;
       ]

let () = run_test_tt_main suite
