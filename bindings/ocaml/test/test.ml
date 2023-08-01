open Opendal
open OUnit2

let test_check_result = function
  | Ok data -> data
  | Error err -> assert_failure err

let new_test_block_operator () : blocking_operator =
  let cfgs = [ ("root", "/tmp/opendal/test") ] in
  test_check_result (new_blocking_operator "fs" cfgs)

let test_new_block_operator _ = ignore new_test_block_operator

let test_create_dir_and_remove_all _ =
  let bo = new_test_block_operator () in
  ignore (test_check_result (blocking_create_dir bo "/testdir/"));
  ignore
    (test_check_result
       (blocking_write bo "/testdir/foo" (Bytes.of_string "bar")));
  ignore
    (test_check_result
       (blocking_write bo "/testdir/bar" (Bytes.of_string "foo")));
  ignore (test_check_result (blocking_remove_all bo "/testdir/"))

let test_block_write_and_read _ =
  let bo = new_test_block_operator () in
  ignore
    (test_check_result
       (blocking_write bo "tempfile" (Bytes.of_string "helloworld")));
  let data = test_check_result (blocking_read bo "tempfile") in
  assert_equal "helloworld"
    (data |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string)

let test_copy_and_read _ =
  let bo = new_test_block_operator () in
  let data = "helloworld" in
  ignore (test_check_result (blocking_write bo "foo" (Bytes.of_string data)));
  ignore (test_check_result (blocking_copy bo "foo" "bar"));
  let got_res = test_check_result (blocking_read bo "bar") in
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
