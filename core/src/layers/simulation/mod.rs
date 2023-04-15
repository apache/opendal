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

#[cfg(madsim)]
mod mock;
#[cfg(not(madsim))]
mod real;

#[cfg(madsim)]
pub use mock::MadsimLayer;
#[cfg(madsim)]
pub use mock::SimServer;
#[cfg(not(madsim))]
pub use real::MadsimLayer;

#[cfg(madsim)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::{services, EntryMode, Operator};
    use madsim::{net::NetSim, runtime::Handle, time::sleep};
    use std::time::Duration;

    //  RUSTFLAGS="--cfg madsim" cargo test layers::simulation::test::test_madsim_layer
    #[madsim::test]
    async fn test_madsim_layer() {
        let handle = Handle::current();
        let ip1 = "10.0.0.1".parse().unwrap();
        let ip2 = "10.0.0.2".parse().unwrap();
        let sim_server_socket = "10.0.0.1:2379".parse().unwrap();
        let server = handle.create_node().name("server").ip(ip1).build();
        let client = handle.create_node().name("client").ip(ip2).build();

        server.spawn(async move {
            SimServer::serve(sim_server_socket).await.unwrap();
        });
        sleep(Duration::from_secs(1)).await;

        let handle = client.spawn(async move {
            let mut builder = services::Fs::default();
            builder.root(".");
            let op = Operator::new(builder)
                .unwrap()
                .layer(MadsimLayer::new(sim_server_socket))
                .finish();

            let path = "hello.txt";
            let data = "Hello, World!";
            op.write(path, data).await.unwrap();
            assert_eq!(data.as_bytes(), op.read(path).await.unwrap());
        });
        handle.await.unwrap();
    }
}
