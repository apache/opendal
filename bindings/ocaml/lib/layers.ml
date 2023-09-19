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

open Core

let new_retry_layer ?(factor : float option) ?(jitter : bool = false)
    ?(max_delay : Time.Span.t option) ?(min_delay : Time.Span.t option)
    max_times =
  let t_option_to_ms opt = Option.map opt ~f:(fun d -> Time.Span.to_ms d) in
  let max_delay = t_option_to_ms max_delay in
  let min_delay = t_option_to_ms min_delay in
  Opendal_core.Layers.new_retry_layer max_times factor jitter max_delay
    min_delay

let new_immutable_index_layer = Opendal_core.Layers.new_immutable_index_layer
let new_concurrent_limit_layer = Opendal_core.Layers.new_concurrent_limit_layer

let new_timeout_layer ?(speed : int64 option) timeout =
  let timeout =
    Option.map timeout ~f:(fun timeout -> Time.Span.to_ms timeout)
  in
  Opendal_core.Layers.new_timeout_layer timeout speed
