#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

azurite_version="${AZURITE_VERSION:-3.38.0}"
azurite_log="${RUNNER_TEMP:-/tmp}/azurite-blob.log"
azurite_data_dir="${RUNNER_TEMP:-/tmp}/azurite-blob-data"

mkdir -p "$(dirname "${azurite_log}")"
mkdir -p "${azurite_data_dir}"

npm install -g "azurite@${azurite_version}" --no-audit --no-fund --loglevel=error

# Azure rejects a ranged GET starting at EOF. Keep full GETs of empty blobs valid.
# Remove this patch once https://github.com/Azure/Azurite/issues/318 is fixed in the pinned release.
if [[ "${azurite_version}" == 3.38.0 ]]; then
  azurite_handler="$(npm root -g)/azurite/dist/src/blob/handlers/BlobHandler.js"
  patch --batch --forward --fuzz=0 "${azurite_handler}" <<'PATCH'
--- BlobHandler.js
+++ BlobHandler.js
@@ -704,6 +704,5 @@
         const rangeStart = rangesParts ? rangesParts[0] : 0;
         let rangeEnd = rangesParts ? rangesParts[1] : Infinity;
-        // Start Range is bigger than blob length
-        if (rangeStart > blob.properties.contentLength) {
+        if (rangesParts && rangeStart >= blob.properties.contentLength) {
             throw StorageErrorFactory_1.default.getInvalidPageRange2(context.contextId, `bytes */${blob.properties.contentLength}`);
         }
PATCH
fi

nohup azurite-blob \
  --location "${azurite_data_dir}" \
  --blobHost 0.0.0.0 \
  --blobPort 10000 \
  --skipApiVersionCheck \
  --silent \
  >"${azurite_log}" 2>&1 &
azurite_pid="$!"

for _ in {1..30}; do
  if bash -c "</dev/tcp/127.0.0.1/10000" 2>/dev/null; then
    echo "Azurite ${azurite_version} is ready on 127.0.0.1:10000"
    exit 0
  fi

  if ! kill -0 "${azurite_pid}" 2>/dev/null; then
    echo "Azurite exited before becoming ready"
    cat "${azurite_log}"
    exit 1
  fi

  sleep 1
done

echo "Azurite did not become ready in time"
cat "${azurite_log}"
exit 1
