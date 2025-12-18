use std::env;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=FDB_CLIENT_LIB_PATH");

    #[cfg(target_os = "macos")]
    {
        let lib_dir = env::var_os("FDB_CLIENT_LIB_PATH")
            .map(PathBuf::from)
            .or_else(find_fdb_client_lib_dir_macos);

        if let Some(path) = lib_dir {
            println!("cargo:rustc-link-search=native={}", path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path.display());
        }
    }
}

#[cfg(target_os = "macos")]
fn find_fdb_client_lib_dir_macos() -> Option<PathBuf> {
    const CANDIDATES: &[&str] = &[
        "/opt/homebrew/opt/foundationdb/lib",
        "/opt/homebrew/lib",
        "/usr/local/opt/foundationdb/lib",
        "/usr/local/lib",
    ];

    for dir in CANDIDATES {
        let dir = Path::new(dir);
        if dir.join("libfdb_c.dylib").exists() {
            return Some(dir.to_path_buf());
        }
    }

    None
}
