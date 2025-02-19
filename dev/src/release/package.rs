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

use crate::workspace_dir;
use semver::Version;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Package {
    name: String,
    path: PathBuf,
    version: Version,
    dependencies: Vec<Package>,
}

impl Package {
    pub fn dependencies(&self) -> &[Package] {
        &self.dependencies
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn make_prefix(&self) -> String {
        format!(
            "apache-opendal-{}-{}-src",
            self.name.replace("/", "-"),
            self.version
        )
    }
}

fn make_package(path: &str, version: &str, dependencies: Vec<Package>) -> Package {
    let name = path.to_string();
    let path = workspace_dir().join(path);
    let version = Version::parse(version).unwrap();
    Package {
        name,
        path,
        version,
        dependencies,
    }
}

/// List all packages that are ready for release.
pub fn all_packages() -> Vec<Package> {
    let core = make_package("core", "0.52.0", vec![]);

    // Integrations
    let cloud_filter = make_package("integrations/cloud_filter", "0.0.6", vec![core.clone()]);
    let compact = make_package("integrations/compat", "1.0.4", vec![core.clone()]);
    let dav_server = make_package("integrations/dav-server", "0.4.0", vec![core.clone()]);
    let fuse3 = make_package("integrations/fuse3", "0.0.13", vec![core.clone()]);
    let object_store = make_package("integrations/object_store", "0.50.0", vec![core.clone()]);
    let parquet = make_package("integrations/parquet", "0.4.0", vec![core.clone()]);
    let unftp_sbe = make_package("integrations/unftp-sbe", "0.0.13", vec![core.clone()]);

    // Binaries
    let oay = make_package("bin/oay", "0.41.17", vec![core.clone(), dav_server.clone()]);
    let ofs = make_package(
        "bin/ofs",
        "0.0.18",
        vec![core.clone(), fuse3.clone(), cloud_filter.clone()],
    );
    let oli = make_package("bin/oli", "0.41.17", vec![core.clone()]);

    // Bindings
    let c = make_package("bindings/c", "0.45.5", vec![core.clone()]);
    let cpp = make_package("bindings/cpp", "0.45.17", vec![core.clone()]);
    let java = make_package("bindings/java", "0.47.9", vec![core.clone()]);
    let nodejs = make_package("bindings/nodejs", "0.47.11", vec![core.clone()]);
    let python = make_package("bindings/python", "0.45.16", vec![core.clone()]);

    vec![
        core,
        cloud_filter,
        compact,
        dav_server,
        fuse3,
        object_store,
        parquet,
        unftp_sbe,
        oay,
        ofs,
        oli,
        c,
        cpp,
        java,
        nodejs,
        python,
    ]
}

pub fn update_package_version(package: &Package) -> bool {
    match package.name.as_str() {
        "core" => update_cargo_version(&package.path, &package.version),
        "integrations/cloud_filter" => update_cargo_version(&package.path, &package.version),
        "integrations/compat" => update_cargo_version(&package.path, &package.version),
        "integrations/dav-server" => update_cargo_version(&package.path, &package.version),
        "integrations/fuse3" => update_cargo_version(&package.path, &package.version),
        "integrations/object_store" => update_cargo_version(&package.path, &package.version),
        "integrations/parquet" => update_cargo_version(&package.path, &package.version),
        "integrations/unftp-sbe" => update_cargo_version(&package.path, &package.version),
        "bin/oay" => update_cargo_version(&package.path, &package.version),
        "bin/ofs" => update_cargo_version(&package.path, &package.version),
        "bin/oli" => update_cargo_version(&package.path, &package.version),

        "bindings/c" => false,   // C bindings has no version to update
        "bindings/cpp" => false, // C++ bindings has no version to update
        "bindings/lua" => false, // Lua bindings has no version to update

        "bindings/python" => update_cargo_version(&package.path, &package.version),
        "bindings/java" => update_maven_version(&package.path, &package.version),
        "bindings/nodejs" => update_nodejs_version(&package.path, &package.version),

        name => panic!("unknown package: {}", name),
    }
}

fn update_cargo_version(path: &Path, version: &Version) -> bool {
    let path = path.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&path).unwrap();
    let mut manifest = toml_edit::DocumentMut::from_str(manifest.as_str()).unwrap();

    let old_version = match manifest["package"]["version"].as_str() {
        Some(version) => Version::parse(version).unwrap(),
        None => panic!("missing version for package: {}", path.display()),
    };

    if &old_version != version {
        manifest["package"]["version"] = toml_edit::value(version.to_string());
        std::fs::write(&path, manifest.to_string()).unwrap();
        println!(
            "updating version for package: {} from {} to {}",
            path.display(),
            old_version,
            version
        );
        true
    } else {
        false
    }
}

fn update_maven_version(path: &Path, version: &Version) -> bool {
    let path = path.join("pom.xml");
    let manifest = std::fs::read_to_string(&path).unwrap();

    let old_version_matcher = r#"<version>\d+\.\d+\.\d+</version> <!-- update version number -->"#;
    let new_version_string = format!("<version>{version}</version> <!-- update version number -->");

    let new_manifest = regex::Regex::new(old_version_matcher)
        .unwrap()
        .replace_all(manifest.as_str(), new_version_string.as_str());
    if manifest != new_manifest {
        std::fs::write(&path, new_manifest.as_bytes()).unwrap();
        println!(
            "updating version for package: {} to {}",
            path.display(),
            version
        );
        true
    } else {
        false
    }
}

fn update_nodejs_version(path: &Path, version: &Version) -> bool {
    let mut updated = false;

    for entry in ignore::Walk::new(path) {
        let entry = entry.unwrap();
        if entry.file_name() != "package.json" {
            continue;
        }
        let manifest = std::fs::read_to_string(entry.path()).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();

        let value = manifest.pointer_mut("/version").unwrap();
        let old_version = Version::parse(value.as_str().unwrap()).unwrap();
        if &old_version != version {
            *value = serde_json::Value::String(version.to_string());
            let mut new_manifest = serde_json::to_string_pretty(&manifest).unwrap();
            new_manifest.push('\n');
            std::fs::write(entry.path(), new_manifest).unwrap();
            println!(
                "updating version for package: {} from {} to {}",
                entry.path().display(),
                old_version,
                version
            );
            updated = true;
        }
    }

    updated
}
