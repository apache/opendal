# Apache OpenDAL™ Cloud Filter Integration

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/test_behavior_integration_cloud_filter.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/cloud_filter_opendal.svg
[crates.io]: https://crates.io/crates/cloud_filter_opendal
[crate downloads]: https://img.shields.io/crates/d/cloud_filter_opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`cloud_filter_opendal` integrates OpenDAL with [cloud sync engines](https://learn.microsoft.com/en-us/windows/win32/cfapi/build-a-cloud-file-sync-engine). It provides a way to access various cloud storage on Windows.

Note that `cloud_filter_opendal` is a read-only service, and it is not recommended to use it in production.

## Example

```rust
use anyhow::Result;
use cloud_filter::root::PopulationType;
use cloud_filter::root::SecurityId;
use cloud_filter::root::Session;
use cloud_filter::root::SyncRootIdBuilder;
use cloud_filter::root::SyncRootInfo;
use opendal::services;
use opendal::Operator;
use tokio::runtime::Handle;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    // Create any service desired
    let op = Operator::from_iter::<services::S3>([
        ("bucket".to_string(), "my_bucket".to_string()),
        ("access_key".to_string(), "my_access_key".to_string()),
        ("secret_key".to_string(), "my_secret_key".to_string()),
        ("endpoint".to_string(), "my_endpoint".to_string()),
        ("region".to_string(), "my_region".to_string()),
    ])?
    .finish();

    let client_path = std::env::var("CLIENT_PATH").expect("$CLIENT_PATH is set");

    // Create a sync root id
    let sync_root_id = SyncRootIdBuilder::new("cloud_filter_opendal")
        .user_security_id(SecurityId::current_user()?)
        .build();

    // Register the sync root if not exists
    if !sync_root_id.is_registered()? {
        sync_root_id.register(
            SyncRootInfo::default()
                .with_display_name("OpenDAL Cloud Filter")
                .with_population_type(PopulationType::Full)
                .with_icon("shell32.dll,3")
                .with_version("1.0.0")
                .with_recycle_bin_uri("http://cloudmirror.example.com/recyclebin")?
                .with_path(&client_path)?,
        )?;
    }

    let handle = Handle::current();
    let connection = Session::new().connect_async(
        &client_path,
        cloud_filter_opendal::CloudFilter::new(op, client_path.clone().into()),
        move |f| handle.block_on(f),
    )?;

    signal::ctrl_c().await?;

    // Drop the connection before unregister the sync root
    drop(connection);
    sync_root_id.unregister()?;

    Ok(())
}
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
