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

let new_operator = OpendalCore.Operator.operator
let is_exist = OpendalCore.Operator.blocking_is_exist
let create_dir = OpendalCore.Operator.blocking_create_dir
let read = OpendalCore.Operator.blocking_read
let write = OpendalCore.Operator.blocking_write
let copy = OpendalCore.Operator.blocking_copy
let rename = OpendalCore.Operator.blocking_rename
let delete = OpendalCore.Operator.blocking_delete
let remove = OpendalCore.Operator.blocking_remove
let remove_all = OpendalCore.Operator.blocking_remove_all
