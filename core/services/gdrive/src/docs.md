## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] rename
- [ ] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Behavior Notes

Google Drive allows duplicate file or directory names under the same parent.
When multiple entries match the same path, OpenDAL resolves the most recently
modified match, falling back to the newer creation time if needed.

Concurrent directory creation through an operator and its clones reuses work for shared ancestors and allows independent paths to progress concurrently.

### Resumable uploads and retries

Writes up to 4 MiB use a single upload request. Larger writes use sequential
resumable uploads with 4 MiB chunks, including when overwriting an existing file.
Stream input through a writer to avoid holding the whole file in memory; the
service buffers a trailing chunk and the current input buffer.

#### Enable automatic retries

Add OpenDAL's
[`RetryLayer`](https://docs.rs/opendal-layer-retry/latest/opendal_layer_retry/struct.RetryLayer.html)
before creating a writer. `Operator::new` does not install it automatically.
The examples below use `opendal-core`, `opendal-service-gdrive`, and
`opendal-layer-retry`, plus Tokio with the `macros`, `rt-multi-thread`, `fs`, and
`io-util` features. Applications using the `opendal` facade can enable
`services-gdrive` and `layers-retry`, and import `Operator`, `services::Gdrive`,
and `layers::RetryLayer` from `opendal` instead.

Configure the retry policy once on the operator. This policy allows up to three
retries per failed operation or writer call. This small upload uses a single
request; a temporary failure retries that whole request:

```rust,no_run
use std::time::Duration;

use opendal_core::{Operator, Result};
use opendal_layer_retry::RetryLayer;
use opendal_service_gdrive::Gdrive;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = Gdrive::default()
        .root("/backups")
        .access_token("<access_token>");
    let op = Operator::new(builder)?.layer(
        RetryLayer::new()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(3),
    );

    op.write("hello.txt", "Hello, Google Drive!").await?;
    Ok(())
}
```

#### Stream a file through the same operator

Call `upload_file(&op, "video.mp4", "video.mp4").await?` with the configured
operator above from an async function.
This example reads the local file in 1 MiB buffers. GDrive groups the input into
4 MiB upload chunks and starts a resumable session when the upload exceeds 4 MiB:

```rust,no_run
use opendal_core::Operator;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

async fn upload_file(
    op: &Operator,
    local_path: &str,
    remote_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::open(local_path).await?;
    let mut writer = op.writer(remote_path).await?;
    let mut buffer = vec![0; 1024 * 1024];

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        writer.write(buffer[..n].to_vec()).await?;
    }

    // Send the remaining bytes and finalize the upload.
    writer.close().await?;
    Ok(())
}
```

`RetryLayer` retries temporary errors from `write` and `close` with backoff,
keeping the same writer and upload session. On a retry after an uncertain upload
response, the GDrive writer queries the session's persisted offset and sends the
remaining bytes, or recognizes that the final upload already completed. Without
`RetryLayer`, the service returns the error without automatically retrying it.
Permanent errors and exhausted retries are returned to the caller through `?`;
the example stops using that writer after an error. Keep one writer for the whole
upload rather than creating a new writer for each chunk or retry.

For single-request uploads, `RetryLayer` retries the whole upload; session-based
recovery applies only to resumable uploads. Upload sessions belong to the writer
and cannot be recovered after the writer is dropped or the process exits. An
expired session returns an error instead of restarting the upload.

## Configuration

Use [`crate::GdriveConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Credentials related

#### Just provide Access Token (Temporary)

- `access_token`: set the access_token for google drive api
Please notice its expiration.

#### Or provide Client ID and Client Secret and refresh token (Long Term)

If you want to let OpenDAL to refresh the access token automatically,
please provide the following fields:

- `refresh_token`: set the refresh_token for google drive api
- `client_id`: set the client_id for google drive api
- `client_secret`: set the client_secret for google drive api

OpenDAL is a library, it cannot do the first step of OAuth2 for you.
You need to get authorization code from user by calling GoogleDrive's authorize url
and exchange it for refresh token.

Make sure you have enabled Google Drive API in your Google Cloud Console.
And your OAuth scope contains `https://www.googleapis.com/auth/drive`.

Please refer to [GoogleDrive OAuth2 Flow](https://developers.google.com/identity/protocols/oauth2/)
for more information.

### Shared drives

- `drive_id`: set the id of a shared drive (formerly Team Drive)

By default, OpenDAL resolves `root` inside the user's My Drive. When `drive_id`
is set, OpenDAL resolves `root` inside that shared drive instead and scopes
every lookup and listing to it. The id appears at the end of the shared drive
URL, for example `https://drive.google.com/drive/folders/<drive_id>`.

The authorized user must be a member of the shared drive. Write and create_dir
require the Contributor role or higher. Delete, rename, and copy onto an
existing path move items or send them to the trash, which requires the Content
manager role or higher.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gdrive::Gdrive;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gdrive::default()
        .root("/test")
        .access_token("<token>");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```

### Via Builder with a shared drive

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gdrive::Gdrive;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gdrive::default()
        .drive_id("<drive_id>")
        .root("/test")
        .access_token("<token>");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
