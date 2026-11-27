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

let new_test_memory_operator () : operator =
  test_check_result (Operator.new_operator "memory" [])

let read_string bo path =
  test_check_result (Operator.read bo path)
  |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string

let contains_substring s sub =
  let len_s = String.length s in
  let len_sub = String.length sub in
  let rec check i =
    if i > len_s - len_sub then false
    else if String.sub s i len_sub = sub then true
    else check (i + 1)
  in
  if len_sub = 0 then true else check 0

let assert_utf8_error label = function
  | Ok _ -> assert_failure (label ^ ": expected UTF-8 error, got Ok")
  | Error err ->
      assert_bool
        (Printf.sprintf "%s: expected UTF-8 error, got %S" label err)
        (contains_substring err "UTF-8")

(** Basic operator creation tests *)
let test_new_block_operator _ = ignore new_test_block_operator

(** Test directory creation and recursive removal *)
let test_create_dir_and_remove_all test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  assert_equal true (test_check_result (Operator.create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/foo" (Bytes.of_string "bar")));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/bar" (Bytes.of_string "foo")));
  ignore (test_check_result (Operator.remove_all bo "/testdir/"))

(** Test basic write and read operations *)
let test_block_write_and_read test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let data = test_check_result (Operator.read bo "tempfile") in
  assert_equal "helloworld"
    (data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

(** Test reader functionality *)
let test_operator_reader test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let reader = Operator.reader bo "tempfile" |> test_check_result in
  let data = Bytes.create 5 in
  let i = Operator.Reader.pread reader data 5L |> test_check_result in
  assert_equal 5 i;
  assert_equal "world" (Bytes.to_string data)

(** Test that a read buffer larger than the remaining bytes returns the tail *)
let test_reader_pread_short_read test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let reader = Operator.reader bo "tempfile" |> test_check_result in
  let data = Bytes.make 64 '\000' in
  let n = Operator.Reader.pread reader data 5L |> test_check_result in
  assert_equal 5 n;
  assert_equal "world" (Bytes.sub_string data 0 n)

(** Test that reads at or beyond EOF return zero without failing *)
let test_reader_pread_eof test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let reader = Operator.reader bo "tempfile" |> test_check_result in
  let data = Bytes.make 8 'x' in
  let n = Operator.Reader.pread reader data 10L |> test_check_result in
  assert_equal 0 n;
  assert_equal "xxxxxxxx" (Bytes.to_string data);
  let n = Operator.Reader.pread reader data 100L |> test_check_result in
  assert_equal 0 n

(** Test that a negative offset is rejected instead of wrapped to u64 *)
let test_reader_pread_negative_offset _ =
  let bo = new_test_memory_operator () in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let reader = Operator.reader bo "tempfile" |> test_check_result in
  let data = Bytes.make 8 'x' in
  match Operator.Reader.pread reader data (-1L) with
  | Ok n -> assert_failure (Printf.sprintf "expected error, got %d" n)
  | Error _ -> ()

(** Test writer functionality *)
let test_operator_writer test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  let writer = Operator.writer bo "test_writer_file" |> test_check_result in
  ignore
    (test_check_result
       (Operator.Writer.write writer (Bytes.of_string "Hello ")));
  ignore
    (test_check_result
       (Operator.Writer.write writer (Bytes.of_string "World!")));
  ignore (test_check_result (Operator.Writer.close writer));

  (* Verify the content was written correctly *)
  let data = test_check_result (Operator.read bo "test_writer_file") in
  assert_equal "Hello World!"
    (data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

(** Test that write copies caller-owned bytes instead of retaining them *)
let test_write_copies_caller_bytes _ =
  let bo = new_test_memory_operator () in
  let data = Bytes.of_string "original" in
  ignore (test_check_result (Operator.write bo "copy" data));
  Bytes.set data 0 'X';
  Gc.compact ();
  assert_equal "original" (read_string bo "copy")

(** Test that each writer chunk is copied before it is buffered *)
let test_writer_copies_caller_bytes _ =
  let bo = new_test_memory_operator () in
  let writer = Operator.writer bo "copy" |> test_check_result in
  let first = Bytes.of_string "Hello " in
  let second = Bytes.of_string "World!" in
  ignore (test_check_result (Operator.Writer.write writer first));
  Bytes.set first 0 'J';
  Gc.compact ();
  ignore (test_check_result (Operator.Writer.write writer second));
  Bytes.set second 0 'X';
  Gc.compact ();
  ignore (test_check_result (Operator.Writer.close writer));
  assert_equal "Hello World!" (read_string bo "copy")

(** Test metadata functionality *)
let test_operator_stat test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore
    (test_check_result
       (Operator.write bo "tempfile" (Bytes.of_string "helloworld")));
  let metadata = Operator.stat bo "tempfile" |> test_check_result in
  assert_equal false (Operator.Metadata.is_dir metadata);
  assert_equal true (Operator.Metadata.is_file metadata);
  assert_equal 10L (Operator.Metadata.content_length metadata);
  assert_equal Operator.Metadata.File (Operator.Metadata.mode metadata);
  assert_equal false (Operator.Metadata.is_deleted metadata);
  ignore (test_check_result (Operator.create_dir bo "tempdir/"));
  let dir_metadata = Operator.stat bo "tempdir/" |> test_check_result in
  assert_equal Operator.Metadata.Dir (Operator.Metadata.mode dir_metadata);
  ()

(** Test list functionality *)
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
  let expected = [| "testdir/"; "foo"; "bar" |] in
  List.iter (Array.sort compare) [ expected; actual ];
  assert_equal expected actual;
  assert_equal 3 (Array.length array);
  ()

(** Test streaming lister functionality *)
let test_lister test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  ignore (test_check_result (Operator.create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/file1" (Bytes.of_string "content1")));
  ignore
    (test_check_result
       (Operator.write bo "/testdir/file2" (Bytes.of_string "content2")));

  let lister = Operator.lister bo "testdir/" |> test_check_result in
  let entries = ref [] in

  (* Collect all entries using the lister *)
  let rec collect_entries () =
    match Operator.Lister.next lister with
    | Ok (Some entry) ->
        entries := Operator.Entry.name entry :: !entries;
        collect_entries ()
    | Ok None -> ()
    | Error err -> assert_failure err
  in
  collect_entries ();

  let actual = Array.of_list (List.rev !entries) in
  let expected = [| "testdir/"; "file1"; "file2" |] in
  List.iter (Array.sort compare) [ expected; actual ];
  assert_equal expected actual;
  assert_equal 3 (Array.length actual)

(** Test operator info functionality *)
let test_operator_info test_ctxt =
  let bo = new_test_block_operator test_ctxt in
  let info = Operator.info bo in

  (* Test basic info properties *)
  let scheme = Operator.OperatorInfo.scheme info in
  assert_equal "fs" scheme;

  let root = Operator.OperatorInfo.root info in
  assert_bool "Root should not be empty" (String.length root > 0);

  (* Test capability information *)
  let cap = Operator.OperatorInfo.capability info in
  assert_equal true (Operator.Capability.read cap);
  assert_equal true (Operator.Capability.write cap);
  assert_equal true (Operator.Capability.stat cap);
  assert_equal true (Operator.Capability.list cap);
  assert_equal true (Operator.Capability.create_dir cap);
  assert_equal true (Operator.Capability.delete cap);

  (* Filesystem should support these operations *)
  assert_equal true (Operator.Capability.copy cap);
  assert_equal true (Operator.Capability.rename cap)

(** Test file existence checking *)
let test_is_exist test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Test non-existent file *)
  let exists = Operator.is_exist bo "nonexistent.txt" |> test_check_result in
  assert_equal false exists;

  (* Create a file and test it exists *)
  ignore
    (test_check_result
       (Operator.write bo "testfile.txt" (Bytes.of_string "test")));
  let exists = Operator.is_exist bo "testfile.txt" |> test_check_result in
  assert_equal true exists

(** Test file copy operation *)
let test_copy test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Create source file *)
  let content = "Hello, copy test!" in
  ignore
    (test_check_result
       (Operator.write bo "source.txt" (Bytes.of_string content)));

  (* Copy the file *)
  ignore (test_check_result (Operator.copy bo "source.txt" "destination.txt"));

  (* Verify both files exist and have same content *)
  let source_data = test_check_result (Operator.read bo "source.txt") in
  let dest_data = test_check_result (Operator.read bo "destination.txt") in

  let source_str =
    source_data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string
  in
  let dest_str = dest_data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string in

  assert_equal content source_str;
  assert_equal content dest_str

(** Test file rename operation *)
let test_rename test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Create source file *)
  let content = "Hello, rename test!" in
  ignore
    (test_check_result
       (Operator.write bo "old_name.txt" (Bytes.of_string content)));

  (* Rename the file *)
  ignore (test_check_result (Operator.rename bo "old_name.txt" "new_name.txt"));

  (* Verify old file doesn't exist and new file has correct content *)
  let old_exists = Operator.is_exist bo "old_name.txt" |> test_check_result in
  let new_exists = Operator.is_exist bo "new_name.txt" |> test_check_result in

  assert_equal false old_exists;
  assert_equal true new_exists;

  let new_data = test_check_result (Operator.read bo "new_name.txt") in
  let new_str = new_data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string in
  assert_equal content new_str

(** Test delete operation *)
let test_delete test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Create a file *)
  ignore
    (test_check_result
       (Operator.write bo "to_delete.txt" (Bytes.of_string "delete me")));

  (* Verify it exists *)
  let exists_before =
    Operator.is_exist bo "to_delete.txt" |> test_check_result
  in
  assert_equal true exists_before;

  (* Delete it *)
  ignore (test_check_result (Operator.delete bo "to_delete.txt"));

  (* Verify it's gone *)
  let exists_after =
    Operator.is_exist bo "to_delete.txt" |> test_check_result
  in
  assert_equal false exists_after

(** Test remove (batch delete) operation *)
let test_remove test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Create multiple files *)
  let files = [| "file1.txt"; "file2.txt"; "file3.txt" |] in
  Array.iter
    (fun file ->
      ignore
        (test_check_result
           (Operator.write bo file (Bytes.of_string ("content of " ^ file)))))
    files;

  (* Verify all files exist *)
  Array.iter
    (fun file ->
      let exists = Operator.is_exist bo file |> test_check_result in
      assert_equal true exists)
    files;

  (* Remove all files *)
  ignore (test_check_result (Operator.remove bo files));

  (* Verify all files are gone *)
  Array.iter
    (fun file ->
      let exists = Operator.is_exist bo file |> test_check_result in
      assert_equal false exists)
    files

(** Test operator check functionality *)
let test_check test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Check should succeed for a valid operator *)
  match Operator.check bo with
  | Ok () -> () (* Success is expected *)
  | Error err -> assert_failure ("Check failed: " ^ err)

(** Test multiple operations together *)
let test_complex_workflow test_ctxt =
  let bo = new_test_block_operator test_ctxt in

  (* Create directory structure *)
  ignore (test_check_result (Operator.create_dir bo "project/"));
  ignore (test_check_result (Operator.create_dir bo "project/src/"));
  ignore (test_check_result (Operator.create_dir bo "project/docs/"));

  (* Create some files using writer *)
  let readme_writer =
    Operator.writer bo "project/README.md" |> test_check_result
  in
  ignore
    (test_check_result
       (Operator.Writer.write readme_writer (Bytes.of_string "# My Project\n")));
  ignore
    (test_check_result
       (Operator.Writer.write readme_writer
          (Bytes.of_string "This is a test project.\n")));
  ignore (test_check_result (Operator.Writer.close readme_writer));

  (* Create a source file *)
  ignore
    (test_check_result
       (Operator.write bo "project/src/main.ml"
          (Bytes.of_string "let () = print_endline \"Hello, World!\"")));

  (* List the project directory *)
  let entries = Operator.list bo "project/" |> test_check_result in
  assert_equal 4 (Array.length entries);

  (* project/, src/, docs/, README.md *)

  (* Verify file contents *)
  let readme_data = test_check_result (Operator.read bo "project/README.md") in
  let readme_str =
    readme_data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string
  in
  assert_bool "README should contain project info"
    (contains_substring readme_str "My Project");

  (* Copy the main file *)
  ignore
    (test_check_result
       (Operator.copy bo "project/src/main.ml" "project/src/backup.ml"));

  (* Check capabilities *)
  let info = Operator.info bo in
  let cap = Operator.OperatorInfo.capability info in
  assert_equal true (Operator.Capability.write cap);
  assert_equal true (Operator.Capability.copy cap);

  (* Clean up *)
  ignore (test_check_result (Operator.remove_all bo "project/"))

(** Test that text arguments with invalid UTF-8 return [Error]. *)
let test_invalid_utf8_rejected _ =
  let bad = "\xff" in
  assert_utf8_error "new_operator scheme" (Operator.new_operator bad []);
  assert_utf8_error "new_operator config key"
    (Operator.new_operator "memory" [ (bad, "value") ]);
  assert_utf8_error "new_operator config value"
    (Operator.new_operator "memory" [ ("key", bad) ]);
  assert_utf8_error "new_operator shadowed invalid config value"
    (Operator.new_operator "memory" [ ("root", bad); ("root", "/valid") ]);

  let bo = new_test_memory_operator () in
  ignore
    (test_check_result
       (Operator.write bo "keep.txt" (Bytes.of_string "keep me")));

  assert_utf8_error "list" (Operator.list bo bad);
  assert_utf8_error "lister" (Operator.lister bo bad);
  assert_utf8_error "stat" (Operator.stat bo bad);
  assert_utf8_error "is_exist" (Operator.is_exist bo bad);
  assert_utf8_error "create_dir" (Operator.create_dir bo bad);
  assert_utf8_error "read" (Operator.read bo bad);
  assert_utf8_error "reader" (Operator.reader bo bad);
  assert_utf8_error "write" (Operator.write bo bad (Bytes.of_string "data"));
  assert_utf8_error "writer" (Operator.writer bo bad);
  assert_utf8_error "copy from" (Operator.copy bo bad "dst");
  assert_utf8_error "copy to" (Operator.copy bo "keep.txt" bad);
  assert_utf8_error "rename from" (Operator.rename bo bad "dst");
  assert_utf8_error "rename to" (Operator.rename bo "keep.txt" bad);
  assert_utf8_error "delete" (Operator.delete bo bad);
  assert_utf8_error "remove" (Operator.remove bo [| "keep.txt"; bad |]);
  assert_equal true (Operator.is_exist bo "keep.txt" |> test_check_result);
  assert_utf8_error "remove_all" (Operator.remove_all bo bad)

(** Test that duplicate config keys keep last-wins behavior *)
let test_config_duplicate_key_last_wins _ =
  let bo =
    Operator.new_operator "memory" [ ("root", "/first"); ("root", "/second") ]
    |> test_check_result
  in
  let info = Operator.info bo in
  assert_equal "/second/" (Operator.OperatorInfo.root info)

(* Compile-time check: wrappers that re-include [Capability] or [Metadata]
   must still match the signature of [Opendal.Operator]. This fails to
   type-check if [operator.mli] exposes either module as a module alias. *)
module Wrapped_operator : module type of Opendal.Operator = struct
  include Opendal.Operator

  module Capability = struct
    include Opendal.Operator.Capability
  end

  module Metadata = struct
    include Opendal.Operator.Metadata
  end
end

(** Test that a structurally re-exported Capability module still works *)
let test_capability_wrapper _ =
  let bo = new_test_memory_operator () in
  let cap = Operator.OperatorInfo.capability (Operator.info bo) in
  assert_equal
    (Operator.Capability.read cap)
    (Wrapped_operator.Capability.read cap)

(** Test that a structurally re-exported Metadata module still works *)
let test_metadata_wrapper _ =
  let bo = new_test_memory_operator () in
  ignore (Operator.write bo "wrapped" (Bytes.of_string "x") |> test_check_result);
  let metadata = Operator.stat bo "wrapped" |> test_check_result in
  assert_equal Operator.Metadata.File (Wrapped_operator.Metadata.mode metadata)

let suite =
  "OCaml OpenDAL Test Suite"
  >::: [
         "test_new_block_operator" >:: test_new_block_operator;
         "test_create_dir_and_remove_all" >:: test_create_dir_and_remove_all;
         "test_block_write_and_read" >:: test_block_write_and_read;
         "test_operator_reader" >:: test_operator_reader;
         "test_reader_pread_short_read" >:: test_reader_pread_short_read;
         "test_reader_pread_eof" >:: test_reader_pread_eof;
         "test_reader_pread_negative_offset"
         >:: test_reader_pread_negative_offset;
         "test_operator_writer" >:: test_operator_writer;
         "test_write_copies_caller_bytes" >:: test_write_copies_caller_bytes;
         "test_writer_copies_caller_bytes" >:: test_writer_copies_caller_bytes;
         "test_operator_stat" >:: test_operator_stat;
         "test_list" >:: test_list;
         "test_lister" >:: test_lister;
         "test_operator_info" >:: test_operator_info;
         "test_is_exist" >:: test_is_exist;
         "test_copy" >:: test_copy;
         "test_rename" >:: test_rename;
         "test_delete" >:: test_delete;
         "test_remove" >:: test_remove;
         "test_check" >:: test_check;
         "test_complex_workflow" >:: test_complex_workflow;
         "test_invalid_utf8_rejected" >:: test_invalid_utf8_rejected;
         "test_config_duplicate_key_last_wins"
         >:: test_config_duplicate_key_last_wins;
         "test_capability_wrapper" >:: test_capability_wrapper;
         "test_metadata_wrapper" >:: test_metadata_wrapper;
       ]

let () = run_test_tt_main suite
