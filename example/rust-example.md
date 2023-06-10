# Prerequisites

There need rust toolchain in your local environment, if you haven't set it up, you can refer [here](https://www.rust-lang.org/tools/install) to figure it out.

# Dependencies

put the content below to Cargo.toml

```toml
[package]
name = "opendal-example-rust"
version = "0.1.0"
edition = "2023"

[dependencies]
opendal = { version = "0.34.0", features = ["services-webdav", "services-gdrive"] }
tokio = { version = "1.27", features = ["full"] }
anyhow = { version = "1.0.30", features = ["std"] }
```

# Usage

Here shows how to use OpenDAL to manipulate file on WebDAV server and Google Drive.

```rust
use opendal::Operator;
use opendal::Result;
use opendal::services::Gdrive;
use opendal::services::Webdav;

#[tokio::main]
async fn main() -> Result<()> {

    // create a webdav builder and configure it
    let mut webdav_builder = Webdav::default();
    webdav_builder
        .endpoint("https://dav.jianguoyun.com/dav/test")
        .username("your_username")
        .password("your_password");

    // init an operator
    let webdav_op: Operator = Operator::new(webdav_builder)?.finish();
    let webdav_file_path = "1.txt";

    // read the file and print the content
    let webdav_read = webdav_op.read(webdav_file_path).await?;
    let content = String::from_utf8(webdav_read).unwrap();
    println!("{}", content);

    // write the file
    let _webdav_write = webdav_op.write(webdav_file_path, "who are you").await?;

    // create a google drive builder and configure it
    let mut gdrive_builder = Gdrive::default();

    gdrive_builder
        .access_token("xxx")
        .root("xxx");

    // init an operator
    let gdrive_op: Operator = Operator::new(gdrive_builder)?.finish();
    let gdrive_file_path = "1.txt";

    // write the file and then read it, print the content
    let _gdrive_write = gdrive_op.write(gdrive_file_path, "who are you").await?;
    let gdrive_read = gdrive_op.read(gdrive_file_path).await?;
    let content = String::from_utf8(gdrive_read).unwrap();
    println!("{}", content);

    // delete the file
    let _gdrive_delete = gdrive_op.delete(gdrive_file_path).await?;

    Ok(())
}
```