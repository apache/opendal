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
