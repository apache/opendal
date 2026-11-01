from typing import Final, final

@final
class Scheme:
    AliyunDrive: Final[Scheme]
    Alluxio: Final[Scheme]
    Azblob: Final[Scheme]
    Azdls: Final[Scheme]
    Azfile: Final[Scheme]
    B2: Final[Scheme]
    Cacache: Final[Scheme]
    CloudflareKv: Final[Scheme]
    Cos: Final[Scheme]
    Dashmap: Final[Scheme]
    Dropbox: Final[Scheme]
    Fs: Final[Scheme]
    Ftp: Final[Scheme]
    Gcs: Final[Scheme]
    GcsGrpc: Final[Scheme]
    Gdrive: Final[Scheme]
    Ghac: Final[Scheme]
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
    VercelBlob: Final[Scheme]
    Webdav: Final[Scheme]
    Webhdfs: Final[Scheme]
    YandexDisk: Final[Scheme]
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    @property
    def name(self, /) -> str: ...
    @property
    def value(self, /) -> str: ...
