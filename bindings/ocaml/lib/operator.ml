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

(** Core operator functions *)
let new_operator = Opendal_core.operator
let list = Opendal_core.blocking_list
let lister = Opendal_core.blocking_lister
let stat = Opendal_core.blocking_stat
let is_exist = Opendal_core.blocking_is_exist
let create_dir = Opendal_core.blocking_create_dir
let read = Opendal_core.blocking_read
let reader = Opendal_core.blocking_reader
let write = Opendal_core.blocking_write
let writer = Opendal_core.blocking_writer
let copy = Opendal_core.blocking_copy
let rename = Opendal_core.blocking_rename
let delete = Opendal_core.blocking_delete
let remove = Opendal_core.blocking_remove
let remove_all = Opendal_core.blocking_remove_all
let check = Opendal_core.blocking_check
let info = Opendal_core.operator_info

(** Reader module for reading data *)
module Reader = struct
  let pread = Opendal_core.reader_pread
end

(** Writer module for writing data *)
module Writer = struct
  let write = Opendal_core.writer_write
  let close = Opendal_core.writer_close
end

(** Lister module for streaming directory listings *)
module Lister = struct
  let next = Opendal_core.lister_next
end

(** Metadata module for file/directory information *)
module Metadata = struct
  let is_file = Opendal_core.metadata_is_file
  let is_dir = Opendal_core.metadata_is_dir
  let content_length = Opendal_core.metadata_content_length
  let content_md5 = Opendal_core.metadata_content_md5
  let content_type = Opendal_core.metadata_content_type
  let content_disposition = Opendal_core.metadata_content_disposition
  let etag = Opendal_core.metadata_etag
  let last_modified = Opendal_core.metadata_last_modified
end

(** Entry module for directory entry information *)
module Entry = struct
  let path = Opendal_core.entry_path
  let name = Opendal_core.entry_name
  let metadata = Opendal_core.entry_metadata
end

(** OperatorInfo module for operator information *)
module OperatorInfo = struct
  let name = Opendal_core.operator_info_name
  let scheme = Opendal_core.operator_info_scheme
  let root = Opendal_core.operator_info_root
  let capability = Opendal_core.operator_info_capability
end

(** Capability module for checking supported operations *)
module Capability = struct
  let stat = Opendal_core.capability_stat
  let read = Opendal_core.capability_read
  let write = Opendal_core.capability_write
  let create_dir = Opendal_core.capability_create_dir
  let delete = Opendal_core.capability_delete
  let copy = Opendal_core.capability_copy
  let rename = Opendal_core.capability_rename
  let list = Opendal_core.capability_list
  let list_with_limit = Opendal_core.capability_list_with_limit
  let list_with_start_after = Opendal_core.capability_list_with_start_after
  let list_with_recursive = Opendal_core.capability_list_with_recursive
  let presign = Opendal_core.capability_presign
  let presign_read = Opendal_core.capability_presign_read
  let presign_stat = Opendal_core.capability_presign_stat
  let presign_write = Opendal_core.capability_presign_write
  let shared = Opendal_core.capability_shared
end
