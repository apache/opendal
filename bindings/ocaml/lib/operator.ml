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

let new_operator = Opendal_core.Operator.operator
let list = Opendal_core.Operator.blocking_list
let stat = Opendal_core.Operator.blocking_stat
let is_exist = Opendal_core.Operator.blocking_is_exist
let create_dir = Opendal_core.Operator.blocking_create_dir
let read = Opendal_core.Operator.blocking_read
let reader = Opendal_core.Operator.blocking_reader
let write = Opendal_core.Operator.blocking_write
let copy = Opendal_core.Operator.blocking_copy
let rename = Opendal_core.Operator.blocking_rename
let delete = Opendal_core.Operator.blocking_delete
let remove = Opendal_core.Operator.blocking_remove
let remove_all = Opendal_core.Operator.blocking_remove_all

module Reader = struct
  let read = Opendal_core.Operator.reader_read

  let seek reader pos mode =
    let inner_pos =
      match mode with
      | Unix.SEEK_CUR -> Opendal_core.Seek_from.Current pos
      | Unix.SEEK_END -> Opendal_core.Seek_from.End pos
      | Unix.SEEK_SET -> Opendal_core.Seek_from.Start pos
    in
    Opendal_core.Operator.reader_seek reader inner_pos
end

module Metadata = struct
  let is_file = Opendal_core.Operator.metadata_is_file
  let is_dir = Opendal_core.Operator.metadata_is_dir
  let content_length = Opendal_core.Operator.metadata_content_length
  let content_md5 = Opendal_core.Operator.metadata_content_md5
  let content_type = Opendal_core.Operator.metadata_content_type
  let content_disposition = Opendal_core.Operator.metadata_content_disposition
  let etag = Opendal_core.Operator.metadata_etag
  let last_modified = Opendal_core.Operator.metadata_last_modified
end

module Entry = struct
  let path = Opendal_core.Operator.entry_path
  let name = Opendal_core.Operator.entry_name
  let metadata = Opendal_core.Operator.entry_metadata
end
