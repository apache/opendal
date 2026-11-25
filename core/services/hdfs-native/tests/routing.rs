// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use opendal_core::Operator;
use opendal_service_hdfs_native::HdfsNative;

#[tokio::test]
#[ignore = "requires the HDFS cluster from fixtures/hdfs/docker-compose-hdfs-cluster.yml"]
async fn reads_through_default_and_logical_nameservices() {
    let root = std::env::var("OPENDAL_HDFS_NATIVE_ROOT").unwrap();
    let endpoint = std::env::var("OPENDAL_HDFS_NATIVE_NAME_NODE").unwrap();
    let address = endpoint
        .strip_prefix("hdfs://")
        .unwrap_or(&endpoint)
        .trim_end_matches('/');
    let options = HashMap::from([
        (
            "fs.defaultFS".to_string(),
            "hdfs://opendal-routing".to_string(),
        ),
        (
            "dfs.ha.namenodes.opendal-routing".to_string(),
            "nn1".to_string(),
        ),
        (
            "dfs.namenode.rpc-address.opendal-routing.nn1".to_string(),
            address.to_string(),
        ),
        (
            "dfs.client.failover.max.attempts".to_string(),
            "0".to_string(),
        ),
    ]);
    let direct = Operator::new(HdfsNative::default().root(&root).name_node(&endpoint)).unwrap();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = format!("nameservice-{}-{nanos}.txt", std::process::id());
    direct.write(&path, "fixture data").await.unwrap();

    let mut reads = Vec::new();
    for name_node in ["default", "hdfs://opendal-routing"] {
        let client = Operator::new(
            HdfsNative::default()
                .root(&root)
                .name_node(name_node)
                .options(options.clone()),
        );
        let result = match client {
            Ok(client) => client.read(&path).await,
            Err(error) => Err(error),
        };
        reads.push((name_node, result));
    }
    direct.delete(&path).await.unwrap();

    for (name_node, result) in reads {
        let data = result.unwrap_or_else(|error| panic!("read through {name_node}: {error:?}"));
        assert_eq!(data.to_vec(), b"fixture data", "read through {name_node}");
    }
}
