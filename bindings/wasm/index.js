/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Operator } from './pkg';

let op = new Operator('test', {
  root: "CI/",
  bucket: "opendal-testing",
  region: "ap-northeast-1",
  endpoint: "https://s3.amazonaws.com",
  access_key_id: "AKIA53WT5ZIABZW3WMOP",
  secret_access_key: "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",
});

async function main(){
  console.log(await op.is_exist('test.txt'))
}

main()
