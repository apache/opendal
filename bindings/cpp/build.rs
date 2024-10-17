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

#[cfg(feature = "async")]
mod build_async {
    use std::{
        env::var,
        io,
        path::{Path, PathBuf},
    };

    #[cfg(unix)]
    fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(original: P, link: Q) -> io::Result<()> {
        std::os::unix::fs::symlink(original, link)
    }

    #[cfg(target_os = "windows")]
    fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(original: P, link: Q) -> io::Result<()> {
        std::os::windows::fs::symlink_file(original, link)
    }

    fn symlink_force<P: AsRef<Path>, Q: AsRef<Path>>(original: P, link: Q) -> io::Result<()> {
        if link.as_ref().exists() {
            return std::fs::remove_file(link);
        }

        symlink(original, link)
    }

    pub fn symlink_async_includes() {
        let async_inc = var("DEP_CXX_ASYNC_INCLUDE").unwrap();
        let src_dir = PathBuf::from(async_inc).join("rust");

        let prj_dir = var("CARGO_MANIFEST_DIR").unwrap();
        let dst_dir = PathBuf::from(prj_dir)
            .join("target")
            .join("cxxbridge")
            .join("rust");

        symlink_force(src_dir.join("cxx_async.h"), dst_dir.join("cxx_async.h")).unwrap();
    }
}

fn main() {
    let _ = cxx_build::bridge("src/lib.rs");
    #[cfg(feature = "async")]
    {
        let _ = cxx_build::bridge("src/async.rs");
        build_async::symlink_async_includes();
    }

    println!("cargo:rerun-if-changed=src/lib.rs");
    #[cfg(feature = "async")]
    println!("cargo:rerun-if-changed=src/async.rs");
}
