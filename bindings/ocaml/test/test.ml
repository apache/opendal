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

let new_test_block_operator test_ctxt : operator =
  let cfgs = [ ("root", bracket_tmpdir test_ctxt) ] in
  test_check_result (Operator.new_operator "fs" cfgs)

let test_new_block_operator _ = ignore new_test_block_operator

let test_create_dir_and_remove_all test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore (test_check_result (Operator.create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/foo" (Bytes.of_string "bar")));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/bar" (Bytes.of_string "foo")));
  ignore (test_check_result (Operator.remove_all bo "/testdir/"))

let test_block_write_and_read test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let data = test_check_result (Operator.read bo "tempfile") in
  assert_equal "helloworld"
    (data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

let test_operator_reader test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let reader = Operator.reader bo "tempfile" |> test_check_result in
  let s = Operator.Reader.seek reader 5L SEEK_CUR |> test_check_result in
  assert_equal 5 (Int64.to_int s);
  let data = Bytes.create 5 in
  let i = Operator.Reader.read reader data |> test_check_result in
  assert_equal 5 i;
  assert_equal "world" (Bytes.to_string data)

let test_operator_stat test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let metadata = Operator.stat bo "tempfile" |> test_check_result in
  assert_equal false (Operator.Metadata.is_dir metadata);
  assert_equal true (Operator.Metadata.is_file metadata);
  assert_equal 10L (Operator.Metadata.content_length metadata);
  ()

let test_list test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore (test_check_result (Operator.create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/foo" (Bytes.of_string "bar")));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/bar" (Bytes.of_string "foo")));
  let array = Operator.list bo "testdir/" |> test_check_result in
  let actual = Array.map Operator.Entry.name array in
  let expected = [| "foo"; "bar" |] in
  List.iter (Array.sort compare) [ expected; actual ];
  assert_equal expected actual;
  assert_equal 2 (Array.length array);
  ()

let suite =
  "suite"
  >::: [
         "test_new_block_operator" >:: test_new_block_operator;
         "test_create_dir_and_remove_all" >:: test_create_dir_and_remove_all;
         "test_block_write_and_read" >:: test_block_write_and_read;
         "test_operator_reader" >:: test_operator_reader;
         "test_operator_stat" >:: test_operator_stat;
         "test_list" >:: test_list;
       ]

let () = run_test_tt_main suite
