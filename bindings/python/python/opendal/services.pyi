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

from typing import Final, final

__all__ = ["Scheme"]

@final
class Scheme:
    Akamai: Final[Scheme]
    AliyunDrive: Final[Scheme]
    Alluxio: Final[Scheme]
    Azblob: Final[Scheme]
    Azdls: Final[Scheme]
    Azfile: Final[Scheme]
    B2: Final[Scheme]
    B2s3: Final[Scheme]
    Cacache: Final[Scheme]
    CephRgw: Final[Scheme]
    Cos: Final[Scheme]
    CosS3: Final[Scheme]
    Dashmap: Final[Scheme]
    Dropbox: Final[Scheme]
    Exoscale: Final[Scheme]
    Foyer: Final[Scheme]
    Fs: Final[Scheme]
    Ftp: Final[Scheme]
    Garage: Final[Scheme]
    Gcs: Final[Scheme]
    GcsGrpc: Final[Scheme]
    GcsS3: Final[Scheme]
    Gdrive: Final[Scheme]
    Ghac: Final[Scheme]
    Github: Final[Scheme]
    Gridfs: Final[Scheme]
    HdfsNative: Final[Scheme]
    Hetzner: Final[Scheme]
    Hf: Final[Scheme]
    Http: Final[Scheme]
    IbmCos: Final[Scheme]
    IdriveE2: Final[Scheme]
    Ionos: Final[Scheme]
    Ipfs: Final[Scheme]
    Ipmfs: Final[Scheme]
    Koofr: Final[Scheme]
    LakefsS3: Final[Scheme]
    Memcached: Final[Scheme]
    Memory: Final[Scheme]
    MiniMoka: Final[Scheme]
    Minio: Final[Scheme]
    Moka: Final[Scheme]
    Mongodb: Final[Scheme]
    Mysql: Final[Scheme]
    Neon: Final[Scheme]
    Obs: Final[Scheme]
    Oci: Final[Scheme]
    Onedrive: Final[Scheme]
    Oss: Final[Scheme]
    OssS3: Final[Scheme]
    Ovhcloud: Final[Scheme]
    Persy: Final[Scheme]
    Postgresql: Final[Scheme]
    Qingstor: Final[Scheme]
    R2: Final[Scheme]
    Redb: Final[Scheme]
    Redis: Final[Scheme]
    Rustfs: Final[Scheme]
    S3: Final[Scheme]
    Scaleway: Final[Scheme]
    Seafile: Final[Scheme]
    Seaweedfs: Final[Scheme]
    Sftp: Final[Scheme]
    Sled: Final[Scheme]
    Spaces: Final[Scheme]
    Sqlite: Final[Scheme]
    Storj: Final[Scheme]
    Supabase: Final[Scheme]
    Swift: Final[Scheme]
    SwiftS3api: Final[Scheme]
    Tigris: Final[Scheme]
    Tos: Final[Scheme]
    TosS3: Final[Scheme]
    Upyun: Final[Scheme]
    VercelArtifacts: Final[Scheme]
    Wasabi: Final[Scheme]
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
