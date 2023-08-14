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

#!/bin/bash

directory=$(pwd)/scripts/services/redis-tls/ssl

mkdir -p $directory

          # Create CA

openssl req \
-x509 -new -nodes \
-keyout $directory/ca.key \
-sha256 \
-days 365 \
-out $directory/ca.crt \
-subj '/CN=Test Root CA/C=US/ST=Test/L=Test/O=Opendal'

# Create redis certificate

openssl req \
-new -nodes \
-out $directory/redis.csr \
-keyout $directory/redis.key \
-subj '/CN=Redis certificate/C=US/ST=Test/L=Test/O=Opendal'

cat > $directory/redis.v3.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 \
-req \
-in $directory/redis.csr \
-CA $directory/ca.crt \
-CAkey $directory/ca.key \
-CAcreateserial \
-out $directory/redis.crt \
-days 300 \
-sha256 \
-extfile $directory/redis.v3.ext

chmod 777 $directory/redis.crt $directory/redis.key # allow the redis docker to read these files

sudo cp $directory/ca.crt /usr/local/share/ca-certificates
sudo update-ca-certificates
