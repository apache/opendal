mod package;

pub fn update_version() -> anyhow::Result<()> {
    let packages = package::all_packages();
    for package in packages {
        package::update_package_version(&package);
    }
    Ok(())
}
