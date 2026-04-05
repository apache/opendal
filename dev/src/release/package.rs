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
use toml_edit::{DocumentMut, Item, TableLike};

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
    let core = make_package("core", "0.55.0", vec![]);

    // Integrations
    let dav_server = make_package("integrations/dav-server", "0.7.0", vec![core.clone()]);
    let object_store = make_package("integrations/object_store", "0.55.0", vec![core.clone()]);
    let parquet = make_package("integrations/parquet", "0.8.0", vec![core.clone()]);
    let unftp_sbe = make_package("integrations/unftp-sbe", "0.4.0", vec![core.clone()]);

    // Binaries moved to separate repositories; no longer released from this repo

    // Bindings
    let c = make_package("bindings/c", "0.46.4", vec![core.clone()]);
    let cpp = make_package("bindings/cpp", "0.45.24", vec![core.clone()]);
    let java = make_package("bindings/java", "0.48.2", vec![core.clone()]);
    let nodejs = make_package("bindings/nodejs", "0.49.2", vec![core.clone()]);
    let python = make_package("bindings/python", "0.47.0", vec![core.clone()]);

    vec![
        core,
        dav_server,
        object_store,
        parquet,
        unftp_sbe,
        c,
        cpp,
        java,
        nodejs,
        python,
    ]
}

pub fn update_package_version(package: &Package) -> bool {
    match package.name.as_str() {
        "core" => update_cargo_version(&package.path, &package.version, package.dependencies()),
        "integrations/dav-server" => {
            update_cargo_version(&package.path, &package.version, package.dependencies())
        }
        "integrations/object_store" => {
            update_cargo_version(&package.path, &package.version, package.dependencies())
        }
        "integrations/parquet" => {
            update_cargo_version(&package.path, &package.version, package.dependencies())
        }
        "integrations/unftp-sbe" => {
            update_cargo_version(&package.path, &package.version, package.dependencies())
        }

        "bindings/c" => false,   // C bindings has no version to update
        "bindings/cpp" => false, // C++ bindings has no version to update
        "bindings/lua" => false, // Lua bindings has no version to update

        "bindings/python" => update_cargo_version(&package.path, &package.version, &[]),
        "bindings/java" => update_maven_version(&package.path, &package.version),
        "bindings/nodejs" => update_nodejs_version(&package.path, &package.version),

        name => panic!("unknown package: {name}"),
    }
}

fn update_cargo_version(path: &Path, version: &Version, dependencies: &[Package]) -> bool {
    let path = path.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&path).unwrap();
    let mut manifest = DocumentMut::from_str(manifest.as_str()).unwrap();
    let mut updated = update_manifest_version(&mut manifest, version, &path);

    for dependency in dependencies {
        updated |= update_dependency_version(&mut manifest, dependency);
    }

    if updated {
        std::fs::write(&path, manifest.to_string()).unwrap();
    }

    updated
}

fn update_manifest_version(manifest: &mut DocumentMut, version: &Version, path: &Path) -> bool {
    if let Some(old_version) = manifest["package"]["version"]
        .as_str()
        .map(Version::parse)
        .transpose()
        .unwrap()
    {
        if &old_version != version {
            manifest["package"]["version"] = toml_edit::value(version.to_string());
            println!(
                "updating version for package: {} from {} to {}",
                path.display(),
                old_version,
                version
            );
            return true;
        }

        return false;
    }

    if let Some(old_version) = manifest["workspace"]["package"]["version"]
        .as_str()
        .map(Version::parse)
        .transpose()
        .unwrap()
    {
        if &old_version != version {
            manifest["workspace"]["package"]["version"] = toml_edit::value(version.to_string());
            println!(
                "updating version for package: {} from {} to {}",
                path.display(),
                old_version,
                version
            );
            return true;
        }

        return false;
    }

    panic!("missing version for package: {}", path.display());
}

fn update_dependency_version(manifest: &mut DocumentMut, dependency: &Package) -> bool {
    let Some(crate_name) = dependency.crate_name() else {
        return false;
    };

    update_dependency_version_in_table(manifest.as_table_mut(), crate_name, dependency)
}

fn update_dependency_version_in_table(
    table: &mut dyn TableLike,
    crate_name: &str,
    dependency: &Package,
) -> bool {
    let mut updated = false;

    for table_name in ["dependencies", "dev-dependencies", "build-dependencies"] {
        let Some(dependencies) = table.get_mut(table_name).and_then(Item::as_table_like_mut) else {
            continue;
        };

        updated |= update_dependency_version_in_dependencies(dependencies, crate_name, dependency);
    }

    let Some(targets) = table.get_mut("target").and_then(Item::as_table_like_mut) else {
        return updated;
    };

    for (_, target) in targets.iter_mut() {
        let Some(target) = target.as_table_like_mut() else {
            continue;
        };
        updated |= update_dependency_version_in_table(target, crate_name, dependency);
    }

    updated
}

fn update_dependency_version_in_dependencies(
    dependencies: &mut dyn TableLike,
    crate_name: &str,
    dependency: &Package,
) -> bool {
    let Some(entry) = dependencies.get_mut(crate_name) else {
        return false;
    };
    let Some(entry) = entry.as_table_like_mut() else {
        return false;
    };
    let Some("../../core") = entry.get("path").and_then(Item::as_str) else {
        return false;
    };
    let Some(value) = entry.get_mut("version") else {
        return false;
    };

    let old_version = match value.as_str() {
        Some(version) => match Version::parse(version) {
            Ok(version) => version,
            Err(_) => return false,
        },
        None => panic!("missing dependency version for crate: {crate_name}"),
    };

    if old_version == dependency.version {
        return false;
    }

    *value = toml_edit::value(dependency.version.to_string());
    println!(
        "updating dependency version for crate: {} from {} to {}",
        crate_name, old_version, dependency.version
    );
    true
}

impl Package {
    fn crate_name(&self) -> Option<&'static str> {
        match self.name.as_str() {
            "core" => Some("opendal"),
            _ => None,
        }
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
        // Ignore theme/package.json
        if entry.path().to_str().unwrap().contains("theme") {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_test_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("odev-package-{name}-{nanos}"))
    }

    #[test]
    fn parquet_release_version_matches_manifest() {
        let parquet = all_packages()
            .into_iter()
            .find(|package| package.name() == "integrations/parquet")
            .unwrap();

        assert_eq!(parquet.version, Version::parse("0.8.0").unwrap());
    }

    #[test]
    fn update_manifest_version_supports_workspace_package_version() {
        let mut manifest = DocumentMut::from_str(
            r#"
[workspace.package]
version = "0.55.0"

[package]
name = "opendal"
version = { workspace = true }
"#,
        )
        .unwrap();

        let updated = update_manifest_version(
            &mut manifest,
            &Version::parse("0.56.0").unwrap(),
            Path::new("core/Cargo.toml"),
        );

        assert!(updated);
        assert_eq!(
            manifest["workspace"]["package"]["version"].as_str(),
            Some("0.56.0")
        );
        assert!(!manifest["package"]["version"].is_str());
    }

    #[test]
    fn update_dependency_version_only_touches_release_managed_core_constraints() {
        let dir = temp_test_dir("dependency-sync");
        std::fs::create_dir_all(&dir).unwrap();

        let manifest_path = dir.join("Cargo.toml");
        std::fs::write(
            &manifest_path,
            r#"[package]
name = "demo"
version = "0.8.0"

[dependencies]
opendal = { version = "0.55.0", path = "../../core" }
serde = "1"

[dev-dependencies]
opendal = { version = "0.55.0", path = "../../core", features = ["services-memory"] }

[build-dependencies]
opendal = { version = ">=0", path = "../../core" }

[target.'cfg(unix)'.dependencies]
opendal = { version = "0.55.0", path = "../../core" }

[target.'cfg(windows)'.dependencies]
opendal = { version = "0.55.0", path = "../core" }
"#,
        )
        .unwrap();

        let dependency = Package {
            name: "core".to_string(),
            path: PathBuf::from("core"),
            version: Version::parse("0.56.0").unwrap(),
            dependencies: vec![],
        };

        let updated = update_cargo_version(
            dir.as_path(),
            &Version::parse("0.8.0").unwrap(),
            &[dependency],
        );
        assert!(updated);

        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        let manifest = DocumentMut::from_str(&manifest).unwrap();

        assert_eq!(
            manifest["dependencies"]["opendal"]["version"].as_str(),
            Some("0.56.0")
        );
        assert_eq!(
            manifest["dev-dependencies"]["opendal"]["version"].as_str(),
            Some("0.56.0")
        );
        assert_eq!(
            manifest["target"]["cfg(unix)"]["dependencies"]["opendal"]["version"].as_str(),
            Some("0.56.0")
        );
        assert_eq!(
            manifest["build-dependencies"]["opendal"]["version"].as_str(),
            Some(">=0")
        );
        assert_eq!(
            manifest["target"]["cfg(windows)"]["dependencies"]["opendal"]["version"].as_str(),
            Some("0.55.0")
        );

        std::fs::remove_dir_all(dir).unwrap();
    }
}
