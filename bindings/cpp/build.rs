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

use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

fn copy_if_changed(source: &Path, destination: &Path) -> io::Result<()> {
    let contents = fs::read(source)?;
    if fs::read(destination).is_ok_and(|existing| existing == contents) {
        return Ok(());
    }

    fs::create_dir_all(destination.parent().unwrap())?;
    // cxx-build may have left a symlink to another Cargo build here.
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    fs::write(destination, contents)
}

fn export_bridge(out_dir: &Path, bridge_dir: &Path) -> io::Result<()> {
    let generated = out_dir.join("cxxbridge");
    let mut files = vec![
        ("include", "rust/cxx.h"),
        ("include", "opendal-cpp/src/capability.rs.h"),
        ("sources", "opendal-cpp/src/capability.rs.cc"),
        ("include", "opendal-cpp/src/lib.rs.h"),
        ("sources", "opendal-cpp/src/lib.rs.cc"),
        ("include", "opendal-cpp/src/metadata.rs.h"),
        ("sources", "opendal-cpp/src/metadata.rs.cc"),
    ];
    if cfg!(feature = "async") {
        files.extend([
            ("include", "opendal-cpp/src/async.rs.h"),
            ("sources", "opendal-cpp/src/async.rs.cc"),
        ]);
        let async_include = PathBuf::from(env::var_os("DEP_CXX_ASYNC_INCLUDE").unwrap());
        let header = "rust/cxx_async.h";
        copy_if_changed(&async_include.join(header), &bridge_dir.join(header))?;
    }

    for (kind, file) in files {
        copy_if_changed(&generated.join(kind).join(file), &bridge_dir.join(file))?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let sources = if cfg!(feature = "async") {
        &[
            "src/capability.rs",
            "src/lib.rs",
            "src/metadata.rs",
            "src/async.rs",
        ][..]
    } else {
        &["src/capability.rs", "src/lib.rs", "src/metadata.rs"][..]
    };
    let _ = cxx_build::bridges(sources);

    println!("cargo:rerun-if-env-changed=OPENDAL_CPP_BRIDGE_DIR");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let bridge_dir = if let Some(directory) = env::var_os("OPENDAL_CPP_BRIDGE_DIR") {
        let directory = PathBuf::from(directory);
        // CMake can clean its generated files independently of Cargo.
        println!("cargo:rerun-if-changed={}", directory.display());
        directory
    } else {
        println!("cargo:rerun-if-env-changed=CARGO_TARGET_DIR");
        env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            // OUT_DIR is <target>/<profile>/build/<package>/out.
            .unwrap_or_else(|| out_dir.ancestors().nth(4).unwrap().to_owned())
            .join("cxxbridge")
    };
    export_bridge(&out_dir, &bridge_dir)
}
