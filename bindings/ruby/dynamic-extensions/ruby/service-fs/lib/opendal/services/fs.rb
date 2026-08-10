# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# frozen_string_literal: true

require "opendal"

module OpenDal
  module Services
    module Fs
      MANIFEST = {
        required_runtime_protocol: 1,
        package_id: "opendal-service-fs-poc",
        component_id: "fs",
        native_entry_symbol: "opendal_service_fs_bootstrap_v1"
      }.freeze

      Runtime.register_service(
        MANIFEST.fetch(:package_id),
        MANIFEST.fetch(:component_id),
        MANIFEST.fetch(:native_entry_symbol),
        File.expand_path("fs/_native/libfs_extension.so", __dir__),
        MANIFEST.fetch(:required_runtime_protocol)
      )
    end
  end
end
