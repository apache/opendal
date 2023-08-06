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

type scheme = SchemeStr of string | Scheme of Inner.Scheme.scheme

let new_operator (input : scheme) (args : (string * string) list) =
  match input with
  | SchemeStr str -> Inner.Block_operator.new_blocking_operator_str str args
  | Scheme s -> Inner.Block_operator.new_blocking_operator s args

let is_exist = Inner.Block_operator.blocking_is_exist
let create_dir = Inner.Block_operator.blocking_create_dir
let read = Inner.Block_operator.blocking_read
let write = Inner.Block_operator.blocking_write
let copy = Inner.Block_operator.blocking_copy
let rename = Inner.Block_operator.blocking_rename
let delete = Inner.Block_operator.blocking_delete
let remove = Inner.Block_operator.blocking_remove
let remove_all = Inner.Block_operator.blocking_remove_all
