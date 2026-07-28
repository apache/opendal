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
- [ ] ~~presign~~

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::SftpConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

For security reasons, it doesn't support password login. Use SSH key-based authentication (e.g., configure your public key on the server via `ssh-copy-id` and provide the private key here).

### Authentication

`SftpBackend` authenticates with a public key, in this order:

1. The private key at `key`, when set.
2. The SSH agent advertised by `SSH_AUTH_SOCK`, on Unix.
3. `~/.ssh/id_ed25519`, `~/.ssh/id_ecdsa`, and `~/.ssh/id_rsa`.

Setting `key` disables the fallbacks, so an unusable key fails instead of
silently authenticating as a different identity.

### Host key verification

`known_hosts_strategy` selects how the remote host key is checked against
`~/.ssh/known_hosts`:

- `strict` (default): the key must already be recorded and match. This
  corresponds to `ssh -o StrictHostKeyChecking=yes`.
- `add`: an unknown key is accepted and recorded, while a changed key is
  rejected. This corresponds to `ssh -o StrictHostKeyChecking=accept-new`.
- `accept`: any key is accepted without being recorded. This corresponds to
  `ssh -o StrictHostKeyChecking=no`.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_service_sftp::Sftp;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Sftp::default()
        .endpoint("127.0.0.1")
        .user("test")
        .key("test_key");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
