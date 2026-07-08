# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from os import PathLike
from typing import Final, final

@final
class AliyunDriveConfig(ServiceConfig):
    """Configuration for the `aliyun-drive` service."""

    def __init__(
        self,
        /,
        drive_type: str,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def access_token(self, /) -> str | None:
        """
        The access_token of this backend.
        Solution for client-only purpose.
        #4733 Required if no client_id, client_secret and refresh_token are
        provided.
        """
    @property
    def client_id(self, /) -> str | None:
        """
        The client_id of this backend.
        Required if no access_token is provided.
        """
    @property
    def client_secret(self, /) -> str | None:
        """
        The client_secret of this backend.
        Required if no access_token is provided.
        """
    @property
    def drive_type(self, /) -> str:
        """
        The drive_type of this backend.
        All operations will happen under this type of drive.
        Available values are `default`, `backup` and `resource`.
        Fallback to default if not set or no other drives can be found.
        """
    @property
    def refresh_token(self, /) -> str | None:
        """
        The refresh_token of this backend.
        Required if no access_token is provided.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        The Root of this backend.
        All operations will happen under this root.
        Default to `/` if not set.
        """

@final
class AlluxioConfig(ServiceConfig):
    """Configuration for the `alluxio` service."""

    def __init__(
        self, /, endpoint: str | None = None, root: str | PathLike[str] | None = None
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """
        Endpoint of this backend.
        Endpoint must be full uri, mostly like `http://127.0.0.1:39999`.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        default to `/` if not set.
        """

@final
class AzblobConfig(ServiceConfig):
    """Configuration for the `azblob` service."""

    def __init__(
        self,
        /,
        container: str,
        account_key: str | None = None,
        account_name: str | None = None,
        batch_max_operations: int | None = None,
        encryption_algorithm: str | None = None,
        encryption_key: str | None = None,
        encryption_key_sha256: str | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        sas_token: str | None = None,
        skip_signature: bool | None = None,
    ) -> None: ...
    @property
    def account_key(self, /) -> str | None:
        """The account key of Azblob service backend."""
    @property
    def account_name(self, /) -> str | None:
        """The account name of Azblob service backend."""
    @property
    def batch_max_operations(self, /) -> int | None:
        """
        Deprecated: Azblob delete batch capability is enabled by default with Azure
        Blob's 256-operation batch limit.
        [Deprecated since 0.57.0] Azblob delete batch capability is enabled by
        default with Azure Blob's 256-operation batch limit.
        Use CapabilityOverrideLayer to override delete_max_size for specific
        endpoints.
        """
    @property
    def container(self, /) -> str:
        """The container name of Azblob service backend."""
    @property
    def encryption_algorithm(self, /) -> str | None:
        """The encryption algorithm of Azblob service backend."""
    @property
    def encryption_key(self, /) -> str | None:
        """The encryption key of Azblob service backend."""
    @property
    def encryption_key_sha256(self, /) -> str | None:
        """The encryption key sha256 of Azblob service backend."""
    @property
    def endpoint(self, /) -> str | None:
        """
        The endpoint of Azblob service backend.
        Endpoint must be full uri, e.g.
        - Azblob: `https://accountname.blob.core.windows.net` - Azurite:
        `http://127.0.0.1:10000/devstoreaccount1`.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        The root of Azblob service backend.
        All operations will happen under this root.
        """
    @property
    def sas_token(self, /) -> str | None:
        """The sas token of Azblob service backend."""
    @property
    def skip_signature(self, /) -> bool | None:
        """Skip signature will skip loading credentials and signing requests."""

@final
class AzdlsConfig(ServiceConfig):
    """Configuration for the `azdls` service."""

    def __init__(
        self,
        /,
        filesystem: str,
        account_key: str | None = None,
        account_name: str | None = None,
        authority_host: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        enable_hns: bool | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        sas_token: str | None = None,
        tenant_id: str | None = None,
    ) -> None: ...
    @property
    def account_key(self, /) -> str | None:
        """
        Account key of this backend.
        - required for shared_key authentication.
        """
    @property
    def account_name(self, /) -> str | None:
        """Account name of this backend."""
    @property
    def authority_host(self, /) -> str | None:
        """
        authority_host The authority host of the service principal.
        - required for client_credentials authentication - default value:
        `https://login.microsoftonline.com`.
        """
    @property
    def client_id(self, /) -> str | None:
        """
        client_id The client id of the service principal.
        - required for client_credentials authentication.
        """
    @property
    def client_secret(self, /) -> str | None:
        """
        client_secret The client secret of the service principal.
        - required for client_credentials authentication.
        """
    @property
    def enable_hns(self, /) -> bool | None:
        """
        Whether hierarchical namespace (HNS) is enabled for the storage account.
        When enabled, recursive deletion can use pagination to avoid timeouts on
        large directories.
        - default value: `false`.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint of this backend."""
    @property
    def filesystem(self, /) -> str:
        """Filesystem name of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def sas_token(self, /) -> str | None:
        """
        sas_token The shared access signature token.
        - required for sas authentication.
        """
    @property
    def tenant_id(self, /) -> str | None:
        """
        tenant_id The tenant id of the service principal.
        - required for client_credentials authentication.
        """

@final
class AzfileConfig(ServiceConfig):
    """Configuration for the `azfile` service."""

    def __init__(
        self,
        /,
        share_name: str,
        account_key: str | None = None,
        account_name: str | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        sas_token: str | None = None,
    ) -> None: ...
    @property
    def account_key(self, /) -> str | None:
        """The account key for azfile."""
    @property
    def account_name(self, /) -> str | None:
        """The account name for azfile."""
    @property
    def endpoint(self, /) -> str | None:
        """The endpoint for azfile."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root path for azfile."""
    @property
    def sas_token(self, /) -> str | None:
        """The sas token for azfile."""
    @property
    def share_name(self, /) -> str:
        """The share name for azfile."""

@final
class B2Config(ServiceConfig):
    """Configuration for the `b2` service."""

    def __init__(
        self,
        /,
        bucket: str,
        bucket_id: str,
        application_key: str | None = None,
        application_key_id: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def application_key(self, /) -> str | None:
        """
        ApplicationKey of this backend.
        - If application_key is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def application_key_id(self, /) -> str | None:
        """
        KeyID of this backend.
        - If application_key_id is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def bucket(self, /) -> str:
        """
        Bucket of this backend.
        required.
        """
    @property
    def bucket_id(self, /) -> str:
        """
        Bucket id of this backend.
        required.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        """

@final
class CacacheConfig(ServiceConfig):
    """Configuration for the `cacache` service."""

    def __init__(self, /, datadir: str | None = None) -> None: ...
    @property
    def datadir(self, /) -> str | None:
        """That path to the cacache data directory."""

@final
class CosConfig(ServiceConfig):
    """Configuration for the `cos` service."""

    def __init__(
        self,
        /,
        bucket: str | None = None,
        disable_config_load: bool | None = None,
        enable_versioning: bool | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        secret_id: str | None = None,
        secret_key: str | None = None,
        security_token: str | None = None,
    ) -> None: ...
    @property
    def bucket(self, /) -> str | None:
        """Bucket of this backend."""
    @property
    def disable_config_load(self, /) -> bool | None:
        """Disable config load so that opendal will not load config from."""
    @property
    def enable_versioning(self, /) -> bool | None:
        """
        Deprecated: COS versioning capability is enabled by default.
        [Deprecated since 0.57.0] COS versioning capability is enabled by default
        and this option is no longer needed.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def secret_id(self, /) -> str | None:
        """Secret ID of this backend."""
    @property
    def secret_key(self, /) -> str | None:
        """Secret key of this backend."""
    @property
    def security_token(self, /) -> str | None:
        """
        Security token (a.k.a.
        session token) of this backend.
        This is used for temporary credentials issued by Tencent Cloud STS (e.g.
        `GetFederationToken` / `AssumeRole`).
        When `security_token` is provided, it will be used together with `secret_id`
        and `secret_key` to sign requests, and the `x-cos-security-token` header
        will be attached automatically by the signer.
        If this field is not set, OpenDAL will also fall back to reading the token
        from environment variables `TENCENTCLOUD_TOKEN`,
        `TENCENTCLOUD_SECURITY_TOKEN` or `QCLOUD_SECRET_TOKEN` (unless
        `disable_config_load` is enabled).
        """

@final
class DashmapConfig(ServiceConfig):
    """Configuration for the `dashmap` service."""

    def __init__(self, /, root: str | PathLike[str] | None = None) -> None: ...
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root path of this backend."""

@final
class DropboxConfig(ServiceConfig):
    """Configuration for the `dropbox` service."""

    def __init__(
        self,
        /,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def access_token(self, /) -> str | None:
        """Access token for dropbox."""
    @property
    def client_id(self, /) -> str | None:
        """client_id for dropbox."""
    @property
    def client_secret(self, /) -> str | None:
        """client_secret for dropbox."""
    @property
    def refresh_token(self, /) -> str | None:
        """refresh_token for dropbox."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root path for dropbox."""

@final
class FsConfig(ServiceConfig):
    """Configuration for the `fs` service."""

    def __init__(
        self,
        /,
        atomic_write_dir: str | PathLike[str] | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def atomic_write_dir(self, /) -> str | PathLike[str] | None:
        """Tmp dir for atomic write."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root dir for backend."""

@final
class FtpConfig(ServiceConfig):
    """Configuration for the `ftp` service."""

    def __init__(
        self,
        /,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        user: str | None = None,
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint of this backend."""
    @property
    def password(self, /) -> str | None:
        """Password of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def user(self, /) -> str | None:
        """User of this backend."""

@final
class GcsConfig(ServiceConfig):
    """Configuration for the `gcs` service."""

    def __init__(
        self,
        /,
        bucket: str,
        allow_anonymous: bool | None = None,
        credential: str | None = None,
        credential_path: str | None = None,
        default_storage_class: str | None = None,
        disable_config_load: bool | None = None,
        disable_vm_metadata: bool | None = None,
        endpoint: str | None = None,
        predefined_acl: str | None = None,
        root: str | PathLike[str] | None = None,
        scope: str | None = None,
        service_account: str | None = None,
        skip_signature: bool | None = None,
        token: str | None = None,
    ) -> None: ...
    @property
    def allow_anonymous(self, /) -> bool | None:
        """
        Allow opendal to send requests without signing when credentials are not
        loaded.
        [Deprecated since 0.57.0] Please use `skip_signature` instead of
        `allow_anonymous`.
        """
    @property
    def bucket(self, /) -> str:
        """Bucket name."""
    @property
    def credential(self, /) -> str | None:
        """Credentials string for GCS service OAuth2 authentication."""
    @property
    def credential_path(self, /) -> str | None:
        """Local path to credentials file for GCS service OAuth2 authentication."""
    @property
    def default_storage_class(self, /) -> str | None:
        """The default storage class used by gcs."""
    @property
    def disable_config_load(self, /) -> bool | None:
        """Disable loading configuration from the environment."""
    @property
    def disable_vm_metadata(self, /) -> bool | None:
        """
        Disable attempting to load credentials from the GCE metadata server when
        running within Google Cloud.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint URI of GCS service, default is `https://storage.googleapis.com`."""
    @property
    def predefined_acl(self, /) -> str | None:
        """The predefined acl for GCS."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root URI, all operations happens under `root`."""
    @property
    def scope(self, /) -> str | None:
        """Scope for gcs."""
    @property
    def service_account(self, /) -> str | None:
        """Service Account for gcs."""
    @property
    def skip_signature(self, /) -> bool | None:
        """Skip signature will skip loading credentials and signing requests."""
    @property
    def token(self, /) -> str | None:
        """
        A Google Cloud OAuth2 token.
        Takes precedence over `credential` and `credential_path`.
        """

@final
class GdriveConfig(ServiceConfig):
    """Configuration for the `gdrive` service."""

    def __init__(
        self,
        /,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def access_token(self, /) -> str | None:
        """Access token for gdrive."""
    @property
    def client_id(self, /) -> str | None:
        """Client id for gdrive."""
    @property
    def client_secret(self, /) -> str | None:
        """Client secret for gdrive."""
    @property
    def refresh_token(self, /) -> str | None:
        """Refresh token for gdrive."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root for gdrive."""

@final
class GhacConfig(ServiceConfig):
    """Configuration for the `ghac` service."""

    def __init__(
        self,
        /,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        runtime_token: str | None = None,
        version: str | None = None,
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """The endpoint for ghac service."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root path for ghac."""
    @property
    def runtime_token(self, /) -> str | None:
        """The runtime token for ghac service."""
    @property
    def version(self, /) -> str | None:
        """The version that used by cache."""

@final
class GoosefsConfig(ServiceConfig):
    """Configuration for the `goosefs` service."""

    def __init__(
        self,
        /,
        auth_type: str | None = None,
        auth_username: str | None = None,
        block_size: int | None = None,
        chunk_size: int | None = None,
        master_addr: str | None = None,
        root: str | PathLike[str] | None = None,
        write_type: str | None = None,
    ) -> None: ...
    @property
    def auth_type(self, /) -> str | None:
        """
        Authentication type.
        Supported values: `"nosasl"`, `"simple"`.
        Default: `"simple"` — PLAIN SASL with usernam
        e. `"nosasl"` — skip authentication entirely.
        """
    @property
    def auth_username(self, /) -> str | None:
        """
        Authentication username.
        Used in SIMPLE mode as the login identity.
        Default: current OS user (`$USER` / `$USERNAME`).
        """
    @property
    def block_size(self, /) -> int | None:
        """Block size in bytes for new files (default: 64 MiB)."""
    @property
    def chunk_size(self, /) -> int | None:
        """Chunk size in bytes for streaming RPCs (default: 1 MiB)."""
    @property
    def master_addr(self, /) -> str | None:
        """
        Master address(es) in `host:port` format.
        For single master: `"10.0.0.1:9200"` For HA (comma-separated):
        `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"` When multiple addresses are
        provided, the client uses `PollingMasterInquireClient` to discover the
        Primary Master automatically.
        Resolution precedence at `build()` time (highest → lowest), following
        `goosefs-sdk` `docs/CLIENT_CONFIGURATION.md` §1:
        1. This field (when set on the builder / OpenDAL config map)
        2. `GOOSEFS_MASTER_ADDR` environment variable
        3. `goosefs.master.rpc.addresses` / `goosefs.master.hostname` in
        `goosefs-site.properties` `build()` fails with `ConfigInvalid` only when
        **none** of the above supplies a master address.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root path of this backend.
        All operations will happen under this root.
        Default to `/` if not set.
        """
    @property
    def write_type(self, /) -> str | None:
        """
        Default write type for new files.
        Supported values: `"must_cache"`, `"cache_through"`, `"through"`,
        `"async_through"`.
        Default: `"must_cache"`.
        """

@final
class GridfsConfig(ServiceConfig):
    """Configuration for the `gridfs` service."""

    def __init__(
        self,
        /,
        bucket: str | None = None,
        chunk_size: int | None = None,
        connection_string: str | None = None,
        database: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def bucket(self, /) -> str | None:
        """The bucket name of the MongoDB GridFs service to read/write."""
    @property
    def chunk_size(self, /) -> int | None:
        """
        The chunk size of the MongoDB GridFs service used to break the user file
        into chunks.
        """
    @property
    def connection_string(self, /) -> str | None:
        """The connection string of the MongoDB service."""
    @property
    def database(self, /) -> str | None:
        """The database name of the MongoDB GridFs service to read/write."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The working directory, all operations will be performed under it."""

@final
class HfConfig(ServiceConfig):
    """Configuration for the `hf` service."""

    def __init__(
        self,
        /,
        download_mode: str | None = None,
        endpoint: str | None = None,
        repo_id: str | None = None,
        repo_type: str | None = None,
        revision: str | None = None,
        root: str | PathLike[str] | None = None,
        token: str | None = None,
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """
        Endpoint of the Hugging Face Hub.
        Default is "https://huggingface.co".
        """
    @property
    def repo_id(self, /) -> str | None:
        """
        Repo id of this backend.
        This is required.
        """
    @property
    def revision(self, /) -> str | None:
        """
        Revision of this backend.
        Default is main.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        Can be "/path/to/dir".
        Default is "/".
        """
    @property
    def token(self, /) -> str | None:
        """
        Token of this backend.
        This is optional.
        """

@final
class HttpConfig(ServiceConfig):
    """Configuration for the `http` service."""

    def __init__(
        self,
        /,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        token: str | None = None,
        username: str | None = None,
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint of this backend."""
    @property
    def password(self, /) -> str | None:
        """Password of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def token(self, /) -> str | None:
        """Token of this backend."""
    @property
    def username(self, /) -> str | None:
        """Username of this backend."""

@final
class IpfsConfig(ServiceConfig):
    """Configuration for the `ipfs` service."""

    def __init__(
        self, /, endpoint: str | None = None, root: str | PathLike[str] | None = None
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """IPFS gateway endpoint."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """IPFS root."""

@final
class IpmfsConfig(ServiceConfig):
    """Configuration for the `ipmfs` service."""

    def __init__(
        self, /, endpoint: str | None = None, root: str | PathLike[str] | None = None
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint for ipfs."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root for ipfs."""

@final
class KoofrConfig(ServiceConfig):
    """Configuration for the `koofr` service."""

    def __init__(
        self,
        /,
        email: str,
        endpoint: str,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def email(self, /) -> str:
        """Koofr email."""
    @property
    def endpoint(self, /) -> str:
        """Koofr endpoint."""
    @property
    def password(self, /) -> str | None:
        """
        Password of this backend.
        (Must be the application password).
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        """

@final
class MemcachedConfig(ServiceConfig):
    """Configuration for the `memcached` service."""

    def __init__(
        self,
        /,
        connection_pool_max_size: int | None = None,
        default_ttl: str | None = None,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        username: str | None = None,
    ) -> None: ...
    @property
    def connection_pool_max_size(self, /) -> int | None:
        """
        The maximum number of connections allowed.
        default is 10.
        """
    @property
    def default_ttl(self, /) -> str | None:
        """
        The default ttl for put operations.
        Accepts a humantime duration string (e.g.
        "5s").
        """
    @property
    def endpoint(self, /) -> str | None:
        """
        Network address of the memcached service.
        For example: "tcp://localhost:11211".
        """
    @property
    def password(self, /) -> str | None:
        """Memcached password, optional."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        The working directory of the service.
        Can be "/path/to/dir" default is "/".
        """
    @property
    def username(self, /) -> str | None:
        """Memcached username, optional."""

@final
class MemoryConfig(ServiceConfig):
    """Configuration for the `memory` service."""

    def __init__(self, /, root: str | PathLike[str] | None = None) -> None: ...
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of the backend."""

@final
class MiniMokaConfig(ServiceConfig):
    """Configuration for the `mini-moka` service."""

    def __init__(
        self,
        /,
        max_capacity: int | None = None,
        root: str | PathLike[str] | None = None,
        time_to_idle: str | None = None,
        time_to_live: str | None = None,
    ) -> None: ...
    @property
    def max_capacity(self, /) -> int | None:
        """
        Sets the max capacity of the cache.
        Refer to
        [`mini-moka::sync::CacheBuilder::max_capacity`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.max_capacity).
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root path of this backend."""
    @property
    def time_to_idle(self, /) -> str | None:
        """
        Sets the time to idle of the cache.
        Refer to
        [`mini-moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_idle).
        """
    @property
    def time_to_live(self, /) -> str | None:
        """
        Sets the time to live of the cache.
        Refer to
        [`mini-moka::sync::CacheBuilder::time_to_live`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_live).
        """

@final
class MokaConfig(ServiceConfig):
    """Configuration for the `moka` service."""

    def __init__(
        self,
        /,
        max_capacity: int | None = None,
        name: str | None = None,
        root: str | PathLike[str] | None = None,
        time_to_idle: str | None = None,
        time_to_live: str | None = None,
    ) -> None: ...
    @property
    def max_capacity(self, /) -> int | None:
        """
        Sets the max capacity of the cache.
        Refer to
        [`moka::future::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.max_capacity).
        """
    @property
    def name(self, /) -> str | None:
        """Name for this cache instance."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root path of this backend."""
    @property
    def time_to_idle(self, /) -> str | None:
        """
        Sets the time to idle of the cache.
        Refer to
        [`moka::future::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.time_to_idle).
        """
    @property
    def time_to_live(self, /) -> str | None:
        """
        Sets the time to live of the cache.
        Refer to
        [`moka::future::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/future/struct.CacheBuilder.html#method.time_to_live).
        """

@final
class MongodbConfig(ServiceConfig):
    """Configuration for the `mongodb` service."""

    def __init__(
        self,
        /,
        collection: str | None = None,
        connection_string: str | None = None,
        database: str | None = None,
        key_field: str | None = None,
        root: str | PathLike[str] | None = None,
        value_field: str | None = None,
    ) -> None: ...
    @property
    def collection(self, /) -> str | None:
        """Collection of this backend."""
    @property
    def connection_string(self, /) -> str | None:
        """Connection string of this backend."""
    @property
    def database(self, /) -> str | None:
        """Database of this backend."""
    @property
    def key_field(self, /) -> str | None:
        """Key field of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def value_field(self, /) -> str | None:
        """Value field of this backend."""

@final
class MysqlConfig(ServiceConfig):
    """Configuration for the `mysql` service."""

    def __init__(
        self,
        /,
        connection_string: str | None = None,
        key_field: str | None = None,
        root: str | PathLike[str] | None = None,
        table: str | None = None,
        value_field: str | None = None,
    ) -> None: ...
    @property
    def connection_string(self, /) -> str | None:
        """
        This connection string is used to connect to the mysql service.
        There are url based formats.
        The format of connect string resembles the url format of the mysql client.
        The format is:
        `[scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...`
        - `mysql://user@localhost` - `mysql://user:password@localhost` -
        `mysql://user:password@localhost:3306` -
        `mysql://user:password@localhost:3306/db` For more information, please refer
        to <https://docs.rs/sqlx/latest/sqlx/mysql/struct.MySqlConnectOptions.html>.
        """
    @property
    def key_field(self, /) -> str | None:
        """The key field name for mysql."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root for mysql."""
    @property
    def table(self, /) -> str | None:
        """The table name for mysql."""
    @property
    def value_field(self, /) -> str | None:
        """The value field name for mysql."""

@final
class ObsConfig(ServiceConfig):
    """Configuration for the `obs` service."""

    def __init__(
        self,
        /,
        access_key_id: str | None = None,
        bucket: str | None = None,
        enable_versioning: bool | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        secret_access_key: str | None = None,
    ) -> None: ...
    @property
    def access_key_id(self, /) -> str | None:
        """Access key id for obs."""
    @property
    def bucket(self, /) -> str | None:
        """Bucket for obs."""
    @property
    def enable_versioning(self, /) -> bool | None:
        """
        Deprecated: OBS versioning capability is not controlled by service config.
        [Deprecated since 0.57.0] OBS versioning capability is not controlled by
        this option and this option is no longer needed.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint for obs."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root for obs."""
    @property
    def secret_access_key(self, /) -> str | None:
        """Secret access key for obs."""

@final
class OnedriveConfig(ServiceConfig):
    """Configuration for the `onedrive` service."""

    def __init__(
        self,
        /,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        enable_versioning: bool | None = None,
        refresh_token: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def access_token(self, /) -> str | None:
        """Microsoft Graph API (also OneDrive API) access token."""
    @property
    def client_id(self, /) -> str | None:
        """
        Microsoft Graph API Application (client) ID that is in the Azure's app
        registration portal.
        """
    @property
    def client_secret(self, /) -> str | None:
        """
        Microsoft Graph API Application client secret that is in the Azure's app
        registration portal.
        """
    @property
    def enable_versioning(self, /) -> bool | None:
        """
        Deprecated: OneDrive versioning capability is enabled by default.
        [Deprecated since 0.57.0] OneDrive versioning capability is enabled by
        default and this option is no longer needed.
        """
    @property
    def refresh_token(self, /) -> str | None:
        """Microsoft Graph API (also OneDrive API) refresh token."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root path for the OneDrive service for the file access."""

@final
class OssConfig(ServiceConfig):
    """Configuration for the `oss` service."""

    def __init__(
        self,
        /,
        bucket: str,
        access_key_id: str | None = None,
        access_key_secret: str | None = None,
        addressing_style: str | None = None,
        allow_anonymous: bool | None = None,
        batch_max_operations: int | None = None,
        delete_max_size: int | None = None,
        enable_versioning: bool | None = None,
        endpoint: str | None = None,
        external_id: str | None = None,
        oidc_provider_arn: str | None = None,
        oidc_token_file: str | None = None,
        presign_addressing_style: str | None = None,
        presign_endpoint: str | None = None,
        role_arn: str | None = None,
        role_session_name: str | None = None,
        root: str | PathLike[str] | None = None,
        security_token: str | None = None,
        server_side_encryption: str | None = None,
        server_side_encryption_key_id: str | None = None,
        skip_signature: bool | None = None,
        sts_endpoint: str | None = None,
    ) -> None: ...
    @property
    def access_key_id(self, /) -> str | None:
        """
        Access key id for oss.
        - this field if it's `is_some` - env value: `ALIBABA_CLOUD_ACCESS_KEY_ID`.
        """
    @property
    def access_key_secret(self, /) -> str | None:
        """
        Access key secret for oss.
        - this field if it's `is_some` - env value:
        `ALIBABA_CLOUD_ACCESS_KEY_SECRET`.
        """
    @property
    def addressing_style(self, /) -> str | None:
        """Addressing style for oss."""
    @property
    def allow_anonymous(self, /) -> bool | None:
        """
        Allow anonymous for oss.
        [Deprecated since 0.57.0] Please use `skip_signature` instead of
        `allow_anonymous`.
        """
    @property
    def batch_max_operations(self, /) -> int | None:
        """
        Deprecated: OSS delete batch capability is enabled by default.
        [Deprecated since 0.57.0] OSS delete batch capability is enabled by default.
        Use CapabilityOverrideLayer to override delete_max_size for specific
        endpoints.
        """
    @property
    def bucket(self, /) -> str:
        """Bucket for oss."""
    @property
    def delete_max_size(self, /) -> int | None:
        """
        Deprecated: OSS delete batch capability is enabled by default.
        [Deprecated since 0.57.0] OSS delete batch capability is enabled by default.
        Use CapabilityOverrideLayer to override delete_max_size for specific
        endpoints.
        """
    @property
    def enable_versioning(self, /) -> bool | None:
        """
        Deprecated: OSS versioning capability is enabled by default.
        [Deprecated since 0.57.0] OSS versioning capability is enabled by default
        and this option is no longer needed.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint for oss."""
    @property
    def external_id(self, /) -> str | None:
        """external_id for this backend."""
    @property
    def oidc_provider_arn(self, /) -> str | None:
        """
        `oidc_provider_arn` will be loaded from - this field if it's `is_some` - env
        value: `ALIBABA_CLOUD_OIDC_PROVIDER_ARN`.
        """
    @property
    def oidc_token_file(self, /) -> str | None:
        """
        `oidc_token_file` will be loaded from - this field if it's `is_some` - env
        value: `ALIBABA_CLOUD_OIDC_TOKEN_FILE`.
        """
    @property
    def presign_addressing_style(self, /) -> str | None:
        """Pre sign addressing style for oss."""
    @property
    def presign_endpoint(self, /) -> str | None:
        """Presign endpoint for oss."""
    @property
    def role_arn(self, /) -> str | None:
        """
        If `role_arn` is set, we will use already known config as source credential
        to assume role with `role_arn`.
        - this field if it's `is_some` - env value: `ALIBABA_CLOUD_ROLE_ARN`.
        """
    @property
    def role_session_name(self, /) -> str | None:
        """role_session_name for this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root for oss."""
    @property
    def security_token(self, /) -> str | None:
        """
        `security_token` will be loaded from - this field if it's `is_some` - env
        value: `ALIBABA_CLOUD_SECURITY_TOKEN`.
        """
    @property
    def server_side_encryption(self, /) -> str | None:
        """Server side encryption for oss."""
    @property
    def server_side_encryption_key_id(self, /) -> str | None:
        """Server side encryption key id for oss."""
    @property
    def skip_signature(self, /) -> bool | None:
        """Skip signature will skip loading credentials and signing requests."""
    @property
    def sts_endpoint(self, /) -> str | None:
        """
        `sts_endpoint` will be loaded from - this field if it's `is_some` - env
        value: `ALIBABA_CLOUD_STS_ENDPOINT`.
        """

@final
class PersyConfig(ServiceConfig):
    """Configuration for the `persy` service."""

    def __init__(
        self,
        /,
        datafile: str | None = None,
        index: str | None = None,
        segment: str | None = None,
    ) -> None: ...
    @property
    def datafile(self, /) -> str | None:
        """
        That path to the persy data file.
        The directory in the path must already exist.
        """
    @property
    def index(self, /) -> str | None:
        """That name of the persy index."""
    @property
    def segment(self, /) -> str | None:
        """That name of the persy segment."""

@final
class PostgresqlConfig(ServiceConfig):
    """Configuration for the `postgresql` service."""

    def __init__(
        self,
        /,
        connection_string: str | None = None,
        key_field: str | None = None,
        root: str | PathLike[str] | None = None,
        table: str | None = None,
        value_field: str | None = None,
    ) -> None: ...
    @property
    def connection_string(self, /) -> str | None:
        """
        The URL should be with a scheme of either `postgres://` or `postgresql://`.
        - `postgresql://user@localhost` -
        `postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10`.
        -
        `postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write`
        - `postgresql:///mydb?user=user&host=/var/lib/postgresql` For more
        information, please visit
        <https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgConnectOptions.html>.
        """
    @property
    def key_field(self, /) -> str | None:
        """The key field of postgresql."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        Default to `/` if not set.
        """
    @property
    def table(self, /) -> str | None:
        """The table of postgresql."""
    @property
    def value_field(self, /) -> str | None:
        """The value field of postgresql."""

@final
class RedbConfig(ServiceConfig):
    """Configuration for the `redb` service."""

    def __init__(
        self,
        /,
        datadir: str | None = None,
        root: str | PathLike[str] | None = None,
        table: str | None = None,
    ) -> None: ...
    @property
    def datadir(self, /) -> str | None:
        """Path to the redb data directory."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root for redb."""
    @property
    def table(self, /) -> str | None:
        """The table name for redb."""

@final
class RedisConfig(ServiceConfig):
    """Configuration for the `redis` service."""

    def __init__(
        self,
        /,
        db: int | None,
        cluster_endpoints: str | None = None,
        connection_pool_max_size: int | None = None,
        default_ttl: str | None = None,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        username: str | None = None,
    ) -> None: ...
    @property
    def cluster_endpoints(self, /) -> str | None:
        """
        Network address of the Redis cluster service.
        Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381",
        e.g.
        default is None.
        """
    @property
    def connection_pool_max_size(self, /) -> int | None:
        """
        The maximum number of connections allowed.
        default is 10.
        """
    @property
    def db(self, /) -> int:
        """The number of DBs redis can take is unlimited default is db 0."""
    @property
    def default_ttl(self, /) -> str | None:
        """
        The default ttl for put operations.
        Accepts a humantime duration string (e.g.
        "5s").
        """
    @property
    def endpoint(self, /) -> str | None:
        """
        Network address of the Redis service.
        Can be "tcp://127.0.0.1:6379", e.g.
        default is "tcp://127.0.0.1:6379".
        """
    @property
    def password(self, /) -> str | None:
        """The password for authentication default is None."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        The working directory of the Redis service.
        Can be "/path/to/dir" default is "/".
        """
    @property
    def username(self, /) -> str | None:
        """
        The username to connect redis service.
        default is None.
        """

@final
class S3Config(ServiceConfig):
    """Configuration for the `s3` service."""

    def __init__(
        self,
        /,
        bucket: str,
        access_key_id: str | None = None,
        allow_anonymous: bool | None = None,
        assume_role_duration_seconds: int | None = None,
        assume_role_session_tags: dict[str, str] | None = None,
        batch_max_operations: int | None = None,
        checksum_algorithm: str | None = None,
        default_acl: str | None = None,
        default_storage_class: str | None = None,
        delete_max_size: int | None = None,
        disable_config_load: bool | None = None,
        disable_ec2_metadata: bool | None = None,
        disable_list_objects_v2: bool | None = None,
        disable_stat_with_override: bool | None = None,
        disable_write_with_if_match: bool | None = None,
        enable_request_payer: bool | None = None,
        enable_versioning: bool | None = None,
        enable_virtual_host_style: bool | None = None,
        enable_write_with_append: bool | None = None,
        endpoint: str | None = None,
        external_id: str | None = None,
        region: str | None = None,
        role_arn: str | None = None,
        role_session_name: str | None = None,
        root: str | PathLike[str] | None = None,
        secret_access_key: str | None = None,
        server_side_encryption: str | None = None,
        server_side_encryption_aws_kms_key_id: str | None = None,
        server_side_encryption_customer_algorithm: str | None = None,
        server_side_encryption_customer_key: str | None = None,
        server_side_encryption_customer_key_md5: str | None = None,
        session_token: str | None = None,
        skip_signature: bool | None = None,
    ) -> None: ...
    @property
    def access_key_id(self, /) -> str | None:
        """
        access_key_id of this backend.
        - If access_key_id is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def allow_anonymous(self, /) -> bool | None:
        """
        Allow anonymous will allow opendal to send request without signing when
        credential is not loaded.
        [Deprecated since 0.57.0] Please use `skip_signature` instead of
        `allow_anonymous`.
        """
    @property
    def assume_role_duration_seconds(self, /) -> int | None:
        """assume_role_duration_seconds for this backend."""
    @property
    def assume_role_session_tags(self, /) -> dict[str, str] | None:
        """assume_role_session_tags for this backend."""
    @property
    def batch_max_operations(self, /) -> int | None:
        """
        Deprecated: S3 delete batch capability is enabled by default.
        [Deprecated since 0.57.0] S3 delete batch capability is enabled by default.
        Use CapabilityOverrideLayer to override delete_max_size for specific
        endpoints.
        """
    @property
    def bucket(self, /) -> str:
        """
        Bucket name of this backend.
        required.
        """
    @property
    def checksum_algorithm(self, /) -> str | None:
        """
        Checksum Algorithm to use when sending checksums in HTTP headers.
        This is necessary when writing to AWS S3 Buckets with Object Lock enabled
        for example.
        Available options: - "crc32c" - "md5".
        """
    @property
    def default_acl(self, /) -> str | None:
        """
        Default ACL for new objects.
        Note that some s3 services like minio do not support this option.
        """
    @property
    def default_storage_class(self, /) -> str | None:
        """
        Default storage_class for this backend.
        Available values: - `DEEP_ARCHIVE` - `GLACIER` - `GLACIER_IR` -
        `INTELLIGENT_TIERING` - `ONEZONE_IA` - `EXPRESS_ONEZONE` - `OUTPOSTS` -
        `REDUCED_REDUNDANCY` - `STANDARD` - `STANDARD_IA` S3 compatible services
        don't support all of them.
        """
    @property
    def delete_max_size(self, /) -> int | None:
        """
        Deprecated: S3 delete batch capability is enabled by default.
        [Deprecated since 0.57.0] S3 delete batch capability is enabled by default.
        Use CapabilityOverrideLayer to override delete_max_size for specific
        endpoints.
        """
    @property
    def disable_config_load(self, /) -> bool | None:
        """
        Disable config load so that opendal will not load config from environment.
        For examples: - envs like `AWS_ACCESS_KEY_ID` - files like `~/.aws/config`.
        """
    @property
    def disable_ec2_metadata(self, /) -> bool | None:
        """
        Disable load credential from ec2 metadata.
        This option is used to disable the default behavior of opendal to load
        credential from ec2 metadata, a.k.a., IMDSv2.
        """
    @property
    def disable_list_objects_v2(self, /) -> bool | None:
        """
        OpenDAL uses List Objects V2 by default to list objects.
        However, some legacy services do not yet support V2.
        This option allows users to switch back to the older List Objects V1.
        """
    @property
    def disable_stat_with_override(self, /) -> bool | None:
        """
        Deprecated: S3 stat override capabilities are enabled by default.
        [Deprecated since 0.57.0] S3 stat override capabilities are enabled by
        default.
        Use CapabilityOverrideLayer to override them for specific endpoints.
        """
    @property
    def disable_write_with_if_match(self, /) -> bool | None:
        """
        Deprecated: S3 write with If-Match capability is enabled by default.
        [Deprecated since 0.57.0] S3 write with If-Match capability is enabled by
        default and this option is no longer needed.
        """
    @property
    def enable_request_payer(self, /) -> bool | None:
        """
        Indicates whether the client agrees to pay for the requests made to the S3
        bucket.
        """
    @property
    def enable_versioning(self, /) -> bool | None:
        """
        Deprecated: S3 versioning capability is enabled by default.
        [Deprecated since 0.57.0] S3 versioning capability is enabled by default and
        this option is no longer needed.
        """
    @property
    def enable_virtual_host_style(self, /) -> bool | None:
        """
        Enable virtual host style so that opendal will send API requests in virtual
        host style instead of path style.
        - By default, opendal will send API to
        `https://s3.us-east-1.amazonaws.com/bucket_name` - Enabled, opendal will
        send API to `https://bucket_name.s3.us-east-1.amazonaws.com`.
        """
    @property
    def enable_write_with_append(self, /) -> bool | None:
        """
        Deprecated: S3 append capability is enabled by default.
        [Deprecated since 0.57.0] S3 append capability is enabled by default and
        this option is no longer needed.
        """
    @property
    def endpoint(self, /) -> str | None:
        """
        Endpoint of this backend.
        Endpoint must be full uri, e.g.
        - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
        - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` - Aliyun
        OSS: `https://{region}.aliyuncs.com` - Tencent COS:
        `https://cos.{region}.myqcloud.com` - Minio: `http://127.0.0.1:9000` If user
        inputs endpoint without scheme like "s3.amazonaws.com", we will prepend
        "https://" before it.
        - If endpoint is set, we will take user's input first.
        - If not, we will try to load it from environment.
        - If still not set, default to `https://s3.amazonaws.com`.
        """
    @property
    def external_id(self, /) -> str | None:
        """external_id for this backend."""
    @property
    def region(self, /) -> str | None:
        """
        Region represent the signing region of this endpoint.
        This is required if you are using the default AWS S3 endpoint.
        If using a custom endpoint, - If region is set, we will take user's input
        first.
        - If not, we will try to load it from environment.
        """
    @property
    def role_arn(self, /) -> str | None:
        """
        role_arn for this backend.
        If `role_arn` is set, we will use already known config as source credential
        to assume role with `role_arn`.
        """
    @property
    def role_session_name(self, /) -> str | None:
        """role_session_name for this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        default to `/` if not set.
        """
    @property
    def secret_access_key(self, /) -> str | None:
        """
        secret_access_key of this backend.
        - If secret_access_key is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def server_side_encryption(self, /) -> str | None:
        """
        server_side_encryption for this backend.
        Available values: `AES256`, `aws:kms`.
        """
    @property
    def server_side_encryption_aws_kms_key_id(self, /) -> str | None:
        """
        server_side_encryption_aws_kms_key_id for this backend - If
        `server_side_encryption` set to `aws:kms`, and
        `server_side_encryption_aws_kms_key_id` is not set, S3 will use aws managed
        kms key to encrypt data.
        - If `server_side_encryption` set to `aws:kms`, and
        `server_side_encryption_aws_kms_key_id` is a valid kms key id, S3 will use
        the provided kms key to encrypt data.
        - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an
        error will be returned.
        - If `server_side_encryption` is not `aws:kms`, setting
        `server_side_encryption_aws_kms_key_id` is a noop.
        """
    @property
    def server_side_encryption_customer_algorithm(self, /) -> str | None:
        """
        server_side_encryption_customer_algorithm for this backend.
        Available values: `AES256`.
        """
    @property
    def server_side_encryption_customer_key(self, /) -> str | None:
        """
        server_side_encryption_customer_key for this backend.
        Value: BASE64-encoded key that matches algorithm specified in
        `server_side_encryption_customer_algorithm`.
        """
    @property
    def server_side_encryption_customer_key_md5(self, /) -> str | None:
        """
        Set server_side_encryption_customer_key_md5 for this backend.
        Value: MD5 digest of key specified in `server_side_encryption_customer_key`.
        """
    @property
    def session_token(self, /) -> str | None:
        """
        session_token (aka, security token) of this backend.
        This token will expire after sometime, it's recommended to set session_token
        by hand.
        """
    @property
    def skip_signature(self, /) -> bool | None:
        """Skip signature will skip loading credentials and signing requests."""

@final
class Scheme:
    AliyunDrive: Final[Scheme]
    Alluxio: Final[Scheme]
    Azblob: Final[Scheme]
    Azdls: Final[Scheme]
    Azfile: Final[Scheme]
    B2: Final[Scheme]
    Cacache: Final[Scheme]
    Cos: Final[Scheme]
    Dashmap: Final[Scheme]
    Dropbox: Final[Scheme]
    Fs: Final[Scheme]
    Ftp: Final[Scheme]
    Gcs: Final[Scheme]
    Gdrive: Final[Scheme]
    Ghac: Final[Scheme]
    Goosefs: Final[Scheme]
    Gridfs: Final[Scheme]
    HdfsNative: Final[Scheme]
    Hf: Final[Scheme]
    Http: Final[Scheme]
    Ipfs: Final[Scheme]
    Ipmfs: Final[Scheme]
    Koofr: Final[Scheme]
    Memcached: Final[Scheme]
    Memory: Final[Scheme]
    MiniMoka: Final[Scheme]
    Moka: Final[Scheme]
    Mongodb: Final[Scheme]
    Mysql: Final[Scheme]
    Obs: Final[Scheme]
    Onedrive: Final[Scheme]
    Oss: Final[Scheme]
    Persy: Final[Scheme]
    Postgresql: Final[Scheme]
    Redb: Final[Scheme]
    Redis: Final[Scheme]
    S3: Final[Scheme]
    Seafile: Final[Scheme]
    Sftp: Final[Scheme]
    Sled: Final[Scheme]
    Sqlite: Final[Scheme]
    Swift: Final[Scheme]
    Tos: Final[Scheme]
    Upyun: Final[Scheme]
    VercelArtifacts: Final[Scheme]
    Webdav: Final[Scheme]
    Webhdfs: Final[Scheme]
    YandexDisk: Final[Scheme]
    def __eq__(self, /, other: object) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, /, other: object) -> bool: ...
    @property
    def name(self, /) -> str: ...
    @property
    def value(self, /) -> str: ...

@final
class SeafileConfig(ServiceConfig):
    """Configuration for the `seafile` service."""

    def __init__(
        self,
        /,
        repo_name: str,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        username: str | None = None,
    ) -> None: ...
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint address of this backend."""
    @property
    def password(self, /) -> str | None:
        """Password of this backend."""
    @property
    def repo_name(self, /) -> str:
        """
        repo_name of this backend.
        required.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        """
    @property
    def username(self, /) -> str | None:
        """Username of this backend."""

class ServiceConfig:
    """Base class for all service configs."""

    @property
    def scheme(self, /) -> str:
        """The service scheme this config targets, e.g. ``"s3"``."""

@final
class SledConfig(ServiceConfig):
    """Configuration for the `sled` service."""

    def __init__(
        self,
        /,
        datadir: str | None = None,
        root: str | PathLike[str] | None = None,
        tree: str | None = None,
    ) -> None: ...
    @property
    def datadir(self, /) -> str | None:
        """That path to the sled data directory."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root for sled."""
    @property
    def tree(self, /) -> str | None:
        """The tree for sled."""

@final
class SqliteConfig(ServiceConfig):
    """Configuration for the `sqlite` service."""

    def __init__(
        self,
        /,
        connection_string: str | None = None,
        key_field: str | None = None,
        root: str | PathLike[str] | None = None,
        table: str | None = None,
        value_field: str | None = None,
    ) -> None: ...
    @property
    def connection_string(self, /) -> str | None:
        """
        Set the connection_string of the sqlite service.
        This connection string is used to connect to the sqlite service.
        The format of connect string resembles the url format of the sqlite client:
        - `sqlite::memory:` - `sqlite:data.db` - `sqlite://data.db` For more
        information, please visit
        <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.
        """
    @property
    def key_field(self, /) -> str | None:
        """
        Set the key field name of the sqlite service to read/write.
        Default to `key` if not specified.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Set the working directory, all operations will be performed under it.
        default: "/".
        """
    @property
    def table(self, /) -> str | None:
        """Set the table name of the sqlite service to read/write."""
    @property
    def value_field(self, /) -> str | None:
        """
        Set the value field name of the sqlite service to read/write.
        Default to `value` if not specified.
        """

@final
class SwiftConfig(ServiceConfig):
    """Configuration for the `swift` service."""

    def __init__(
        self,
        /,
        container: str | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        temp_url_hash_algorithm: str | None = None,
        temp_url_key: str | None = None,
        token: str | None = None,
    ) -> None: ...
    @property
    def container(self, /) -> str | None:
        """The container for Swift."""
    @property
    def endpoint(self, /) -> str | None:
        """The endpoint for Swift."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """The root for Swift."""
    @property
    def temp_url_hash_algorithm(self, /) -> str | None:
        """
        The hash algorithm for TempURL signing.
        Supported values: `sha1`, `sha256`, `sha512`.
        Defaults to `sha256`.
        The cluster must have the chosen algorithm in its `tempurl.allowed_digests`
        (check `GET /info`).
        """
    @property
    def temp_url_key(self, /) -> str | None:
        """
        The TempURL key for generating presigned URLs.
        This corresponds to the `X-Account-Meta-Temp-URL-Key` or
        `X-Container-Meta-Temp-URL-Key` header value configured on the Swift account
        or container.
        """
    @property
    def token(self, /) -> str | None:
        """The token for Swift."""

@final
class TosConfig(ServiceConfig):
    """Configuration for the `tos` service."""

    def __init__(
        self,
        /,
        bucket: str,
        access_key_id: str | None = None,
        disable_config_load: bool | None = None,
        endpoint: str | None = None,
        region: str | None = None,
        root: str | PathLike[str] | None = None,
        secret_access_key: str | None = None,
        security_token: str | None = None,
        skip_signature: bool | None = None,
    ) -> None: ...
    @property
    def access_key_id(self, /) -> str | None:
        """
        access_key_id of this backend.
        - If access_key_id is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def bucket(self, /) -> str:
        """
        Bucket name of this backend.
        required.
        """
    @property
    def disable_config_load(self, /) -> bool | None:
        """
        Disable config load so that opendal will not load config from environment.
        For examples: - envs like `TOS_ACCESS_KEY_ID`.
        """
    @property
    def endpoint(self, /) -> str | None:
        """
        Endpoint of this backend.
        Endpoint must be full uri, e.g.
        - TOS: `https://tos-cn-beijing.volces.com` - TOS with region:
        `https://tos-{region}.volces.com` If user inputs endpoint without scheme
        like "tos-cn-beijing.volces.com", we will prepend "https://" before it.
        """
    @property
    def region(self, /) -> str | None:
        """
        Region represent the signing region of this endpoint.
        Required if endpoint is not provided.
        - If region is set, we will take user's input first.
        - If not, we will try to load it from environment.
        - If still not set, default to `cn-beijing`.
        """
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        default to `/` if not set.
        """
    @property
    def secret_access_key(self, /) -> str | None:
        """
        secret_access_key of this backend.
        - If secret_access_key is set, we will take user's input first.
        - If not, we will try to load it from environment.
        """
    @property
    def security_token(self, /) -> str | None:
        """
        security_token of this backend.
        This token will expire after sometime, it's recommended to set
        security_token by hand.
        """
    @property
    def skip_signature(self, /) -> bool | None:
        """Skip signature will skip loading credentials and signing requests."""

@final
class UpyunConfig(ServiceConfig):
    """Configuration for the `upyun` service."""

    def __init__(
        self,
        /,
        bucket: str,
        operator: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
    ) -> None: ...
    @property
    def bucket(self, /) -> str:
        """Bucket address of this backend."""
    @property
    def operator(self, /) -> str | None:
        """Username of this backend."""
    @property
    def password(self, /) -> str | None:
        """Password of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        """

@final
class VercelArtifactsConfig(ServiceConfig):
    """Configuration for the `vercel-artifacts` service."""

    def __init__(
        self,
        /,
        access_token: str | None = None,
        endpoint: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> None: ...
    @property
    def access_token(self, /) -> str | None:
        """The access token for Vercel."""
    @property
    def endpoint(self, /) -> str | None:
        """
        The endpoint for the Vercel artifacts API.
        Defaults to `https://api.vercel.com`.
        """
    @property
    def team_id(self, /) -> str | None:
        """
        The Vercel team ID.
        When set, the `teamId` query parameter is appended to all API requests.
        """
    @property
    def team_slug(self, /) -> str | None:
        """
        The Vercel team slug.
        When set, the `slug` query parameter is appended to all API requests.
        """

@final
class WebdavConfig(ServiceConfig):
    """Configuration for the `webdav` service."""

    def __init__(
        self,
        /,
        disable_copy: bool | None = None,
        disable_create_dir: bool | None = None,
        enable_conditional_read: bool | None = None,
        enable_user_metadata: bool | None = None,
        endpoint: str | None = None,
        password: str | None = None,
        root: str | PathLike[str] | None = None,
        token: str | None = None,
        user_metadata_prefix: str | None = None,
        user_metadata_uri: str | None = None,
        username: str | None = None,
    ) -> None: ...
    @property
    def disable_copy(self, /) -> bool | None:
        """
        Deprecated: WebDAV copy capability is enabled by default.
        [Deprecated since 0.57.0] WebDAV copy capability is enabled by default and
        this option is no longer needed.
        """
    @property
    def disable_create_dir(self, /) -> bool | None:
        """
        Disable automatic parent directory creation before write operations.
        By default, OpenDAL creates parent directories using MKCOL before writing
        files.
        This requires PROPFIND support to check directory existence.
        Some WebDAV-compatible servers (e.g., bazel-remote) don't support PROPFIND
        or don't require explicit directory creation.
        Enable this option to skip the MKCOL calls and write files directly.
        Default: false.
        """
    @property
    def enable_conditional_read(self, /) -> bool | None:
        """
        Enable conditional read support.
        When enabled (the default), OpenDAL forwards the RFC 7232 headers
        `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`
        to the server when callers provide them.
        Some WebDAV-compatible servers (e.g., nginx-dav) don't return ETags in
        PROPFIND or don't honor these headers on GET.
        Setting this to `false` drops the four `read_with_if_*` capabilities, so
        calls like `reader_with(path).if_match(...)` return `ErrorKind::Unsupported`
        locally instead of being silently ignored by the server.
        Default: true.
        """
    @property
    def enable_user_metadata(self, /) -> bool | None:
        """
        Deprecated: WebDAV user metadata capability is enabled by default.
        [Deprecated since 0.57.0] WebDAV user metadata capability is enabled by
        default.
        Use CapabilityOverrideLayer to override write_with_user_metadata for
        endpoints without PROPPATCH support.
        """
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint of this backend."""
    @property
    def password(self, /) -> str | None:
        """Password of this backend."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root of this backend."""
    @property
    def token(self, /) -> str | None:
        """Token of this backend."""
    @property
    def user_metadata_prefix(self, /) -> str | None:
        """
        The XML namespace prefix for user metadata properties.
        This prefix is used in PROPPATCH/PROPFIND XML requests.
        Different servers may require different prefixes.
        Default: "opendal".
        """
    @property
    def user_metadata_uri(self, /) -> str | None:
        """
        The XML namespace URI for user metadata properties.
        This URI uniquely identifies the namespace for custom properties.
        Different servers may require different namespace URIs.
        For example, Nextcloud might work better with its own namespace.
        Default: `https://opendal.apache.org/ns`.
        """
    @property
    def username(self, /) -> str | None:
        """Username of this backend."""

@final
class WebhdfsConfig(ServiceConfig):
    """Configuration for the `webhdfs` service."""

    def __init__(
        self,
        /,
        atomic_write_dir: str | PathLike[str] | None = None,
        delegation: str | None = None,
        disable_list_batch: bool | None = None,
        endpoint: str | None = None,
        root: str | PathLike[str] | None = None,
        user_name: str | None = None,
    ) -> None: ...
    @property
    def atomic_write_dir(self, /) -> str | PathLike[str] | None:
        """atomic_write_dir of this backend."""
    @property
    def delegation(self, /) -> str | None:
        """Delegation token for webhdfs."""
    @property
    def disable_list_batch(self, /) -> bool | None:
        """Disable batch listing."""
    @property
    def endpoint(self, /) -> str | None:
        """Endpoint for webhdfs."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """Root for webhdfs."""
    @property
    def user_name(self, /) -> str | None:
        """Name of the user for webhdfs."""

@final
class YandexDiskConfig(ServiceConfig):
    """Configuration for the `yandex-disk` service."""

    def __init__(
        self, /, access_token: str, root: str | PathLike[str] | None = None
    ) -> None: ...
    @property
    def access_token(self, /) -> str:
        """Yandex disk oauth access_token."""
    @property
    def root(self, /) -> str | PathLike[str] | None:
        """
        Root of this backend.
        All operations will happen under this root.
        """
