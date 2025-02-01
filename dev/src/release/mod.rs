use crate::{find_command, workspace_dir};
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha512};
use std::io::BufReader;

mod package;

pub fn update_version() -> anyhow::Result<()> {
    let packages = package::all_packages();
    let mut updated = false;
    for package in packages {
        updated |= package::update_package_version(&package);
    }
    if !updated {
        println!("all package versions are up-to-date");
    }
    Ok(())
}

pub fn archive_package() -> anyhow::Result<()> {
    std::fs::create_dir_all(workspace_dir().join("dist"))?;

    let workspace_dir = workspace_dir();
    let dist_dir = workspace_dir.join("dist");

    let packages = package::all_packages();
    for package in packages {
        let mut cmd = find_command("git", &workspace_dir);
        cmd.args(["ls-files", "LICENSE", "NOTICE"]);
        cmd.arg(&package.name);
        for dep in &package.dependencies {
            cmd.arg(&dep.name);
        }
        let output = cmd.output().expect("failed to execute git ls-files");
        let output = String::from_utf8_lossy(&output.stdout);
        let files = output.lines().collect::<Vec<_>>();
        archive_and_checksum(&package, &files)?;

        let prefix = format!("apache-opendal-{}-src", package.name.replace("/", "-"));
        let filename = format!("{}.tar.gz", prefix);

        println!("Generate signature for package: {}", package.name);
        let mut cmd = find_command("gpg", &dist_dir);
        cmd.args([
            "--yes",
            "--armor",
            "--output",
            format!("{filename}.asc").as_str(),
            "--detach-sign",
            filename.as_str(),
        ]);
        cmd.output().expect("failed to sign the package");

        println!("Check signature for package: {}", package.name);
        let mut cmd = find_command("gpg", &dist_dir);
        cmd.args([
            "--verify",
            format!("{filename}.asc").as_str(),
            filename.as_str(),
        ]);
        cmd.output()
            .expect("failed to verify the package signature");
    }

    Ok(())
}

fn archive_and_checksum(package: &package::Package, files: &[&str]) -> anyhow::Result<()> {
    println!("archiving package: {}", package.name);

    let prefix = format!("apache-opendal-{}-src", package.name.replace("/", "-"));
    let filename = format!("{}.tar.gz", prefix);
    let tarball = workspace_dir().join("dist").join(&filename);

    {
        let tarball = std::fs::File::create(&tarball)?;
        let encoder = GzEncoder::new(tarball, Compression::default());
        let mut tar = tar::Builder::new(encoder);
        for file in files {
            tar.append_path_with_name(workspace_dir().join(file), format!("{prefix}/{file}"))?;
        }
    }

    {
        let tarball = std::fs::File::open(&tarball)?;
        let mut reader = BufReader::new(tarball);
        let mut hasher = Sha512::new();
        std::io::copy(&mut reader, &mut hasher)?;
        let digest = hasher.finalize();
        let checksum_lines = format!("{digest:x}  {filename}");

        let checksum = workspace_dir()
            .join("dist")
            .join(format!("{}.sha512", filename));
        std::fs::write(checksum, checksum_lines)?;
    }

    Ok(())
}
