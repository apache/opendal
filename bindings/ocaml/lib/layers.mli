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

val new_retry_layer :
  ?factor:float ->
  ?jitter:bool ->
  ?max_delay:Core.Core_private.Span_float.t ->
  ?min_delay:Core.Core_private.Span_float.t ->
  int option ->
  Opendal_core.Layers.layer
(** [new_retry_layer ?factor ?jitter ?max_delay ?min_delay max_times] Add retry for temporary failed operations.
    @param factor Set factor of current backoff. This function will panic if input factor smaller than `1.0`.
    @param jitter Set jitter of current backoff. If jitter is enabled, ExponentialBackoff will add a random jitter in 0 to min_delay current delay
    @param max_delay Set max_delay of current backoff. Delay will not increasing if current delay is larger than max_delay.
    @param min_delay Set min_delay of current backoff.
    @param max_times Set max_times of current backoff. Backoff will return `None` if max times is reaching.
    @return layer
*)

val new_immutable_index_layer : string array -> Opendal_core.Layers.layer
(** [new_immutable_index_layer keys] Add an immutable in-memory index for underlying storage services.
    Especially useful for services without list capability like HTTP.
    @param keys  Index keys array
    @return layer
*)

val new_concurrent_limit_layer : int -> Opendal_core.Layers.layer
(** [new_concurrent_limit_layer permits] Add concurrent request limit.
    Users can control how many concurrent connections could be established
    between OpenDAL and underlying storage services.

    @param permits concurrent limit permits
    @return layer
*)

val new_timeout_layer :
  ?speed:int64 ->
  Core.Core_private.Span_float.t Option.t ->
  Opendal_core.Layers.layer
(** [new_timeout_layer ?speed timeout] Add timeout for every operations.
    For IO operations like `read`, `write`, we will set a timeout for each single IO operation.
    For other operations like `stat`, and `delete`, the timeout is for the whole operation.
    
    @param ?speed (default 1024 bytes per second, aka, 1KiB/s.) 
    @param timeout (default 60 seconds)
    @return layer
*)
