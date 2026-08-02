// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
        let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
        let export_map = manifest_dir.join("exports.map");
        println!("cargo:rerun-if-changed={}", export_map.display());
        println!(
            "cargo:rustc-cdylib-link-arg=-Wl,--version-script={}",
            export_map.display()
        );
    }
}
