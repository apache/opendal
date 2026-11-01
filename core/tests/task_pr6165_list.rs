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

use std::path::PathBuf;

use opendal::{Operator, Result, blocking, services};

#[test]
fn blocking_lists_relative_redb_entries() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create control runtime");
    let _guard = runtime.enter();
    let parent = PathBuf::from(
        std::env::var_os("OPENDAL_TASK_REDB_CONTROL_DIR").expect("owned control directory"),
    );

    for (label, root) in [("flat", "/"), ("rooted", "/tenant/pr6165/")] {
        let path = parent.join(format!("{label}.redb"));
        assert!(!path.exists());
        let op = Operator::new(
            services::Redb::default()
                .datadir(path.to_str().expect("UTF-8 control path"))
                .table("listing")
                .root(root),
        )?;
        let op = blocking::Operator::new(op)?;
        op.write("plain", "plain".to_string())?;
        op.write("nested/file", "nested".to_string())?;

        let entries = op.list("/")?;
        let mut paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["nested/", "plain"]);
        let plain = entries
            .iter()
            .find(|entry| entry.path() == "plain")
            .unwrap();
        assert!(plain.metadata().mode().is_file());
        assert_eq!(plain.metadata().content_length(), 5);

        let entries: Vec<_> = op.lister("/")?.collect::<Result<_>>()?;
        let mut paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["nested/", "plain"]);

        let entries = op.list("nested/")?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path(), "nested/file");
        assert_eq!(entries[0].metadata().content_length(), 6);
        assert_eq!(op.read("nested/file")?.to_vec(), b"nested");
        println!("BLOCKING_REDB root={root} list=2 lister=2 prefix=1 bytes=6");
        drop(op);
        std::fs::remove_file(path).expect("remove owned control database");
    }
    Ok(())
}
