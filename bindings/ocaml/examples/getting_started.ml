(*
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*)

(* ANCHOR: quickstart *)
open Opendal

(* Create an operator for the in-memory service — no credentials needed. *)
let () =
  match Operator.new_operator "memory" [] with
  | Error err -> Printf.eprintf "Failed to create operator: %s\n" err; exit 1
  | Ok op ->

  (* Write a file. *)
  (match Operator.write op "hello.txt" (Bytes.of_string "Hello, World!") with
  | Error err -> Printf.eprintf "Write failed: %s\n" err; exit 1
  | Ok () ->

  (* Read it back — result is a char array. *)
  (match Operator.read op "hello.txt" with
  | Error err -> Printf.eprintf "Read failed: %s\n" err; exit 1
  | Ok content ->
      let text = content |> Array.to_seq |> Bytes.of_seq |> Bytes.to_string in
      Printf.printf "read: %s\n" text;

  (* Inspect metadata. *)
  (match Operator.stat op "hello.txt" with
  | Error err -> Printf.eprintf "Stat failed: %s\n" err; exit 1
  | Ok meta ->
      Printf.printf "size = %Ld bytes\n" (Operator.Metadata.content_length meta);

  (* Delete the file. *)
  (match Operator.delete op "hello.txt" with
  | Error err -> Printf.eprintf "Delete failed: %s\n" err; exit 1
  | Ok () -> ()))))
(* ANCHOR_END: quickstart *)
