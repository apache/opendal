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
    let core = make_package("core", "0.51.1", vec![]);

    // Integrations
    let cloud_filter = make_package("integrations/cloud_filter", "0.0.4", vec![core.clone()]);
    let compact = make_package("integrations/compat", "1.0.2", vec![core.clone()]);
    let dav_server = make_package("integrations/dav-server", "0.3.0", vec![core.clone()]);
    let fuse3 = make_package("integrations/fuse3", "0.0.11", vec![core.clone()]);
    let object_store = make_package("integrations/object_store", "0.49.0", vec![core.clone()]);
    let parquet = make_package("integrations/parquet", "0.3.0", vec![core.clone()]);
    let unftp_sbe = make_package("integrations/unftp-sbe", "0.0.11", vec![core.clone()]);

    // Binaries
    let oay = make_package("bin/oay", "0.41.15", vec![core.clone(), dav_server.clone()]);
    let ofs = make_package(
        "bin/ofs",
        "0.0.16",
        vec![core.clone(), fuse3.clone(), cloud_filter.clone()],
    );
    let oli = make_package("bin/oli", "0.41.15", vec![core.clone()]);

    // Bindings
    let c = make_package("bindings/c", "0.45.3", vec![core.clone()]);
    let cpp = make_package("bindings/cpp", "0.45.15", vec![core.clone()]);
    // let haskell = make_package("bindings/haskell", "0.44.15", vec![core.clone()]);
    // let java = make_package("bindings/java", "0.47.7", vec![core.clone()]);
    let lua = make_package("bindings/lua", "0.1.13", vec![core.clone()]);
    // let nodejs = make_package("bindings/nodejs", "0.47.9", vec![core.clone()]);
    let python = make_package("bindings/python", "0.45.14", vec![core.clone()]);

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
        // haskell,
        // java,
        lua,
        // nodejs,
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
