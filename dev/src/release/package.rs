use crate::workspace_dir;
use semver::Version;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Package {
    name: String,
    path: PathBuf,
    version: Version,
    dependencies: Vec<Package>,
}

fn make_package(path: &str, version: &str, dependencies: Vec<Package>) -> Package {
    let name = path.to_string();
    let path = workspace_dir().join(path).canonicalize().unwrap();
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
    let core = make_package("core", "0.51.0", vec![]);

    // Integrations
    let cloud_filter = make_package("integrations/cloud_filter", "0.0.4", vec![core.clone()]);
    let compact = make_package("integrations/compact", "1.0.2", vec![core.clone()]);
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
    let haskell = make_package("bindings/haskell", "0.44.15", vec![core.clone()]);
    let java = make_package("bindings/java", "0.47.7", vec![core.clone()]);
    let lua = make_package("bindings/lua", "0.1.13", vec![core.clone()]);
    let nodejs = make_package("bindings/nodejs", "0.47.9", vec![core.clone()]);
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
        haskell,
        java,
        lua,
        nodejs,
        python,
    ]
}
