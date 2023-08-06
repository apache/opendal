// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use ::opendal as od;

#[derive(ocaml::FromValue, ocaml::ToValue, Clone, Copy)]
#[ocaml::sig(
    "
| Azblob  (** [Azblob]: Azure Storage Blob services.*)

| Azdfs  (** [Azdfs]: Azure Data Lake Storage Gen2. *)

| Cacache  (** [Cacache]: cacache backend support. *)

| Cos  (** [Cos]: Tencent Cloud Object Storage services. *)

| Dashmap  (** [Dashmap]: dashmap backend support. *)

| Etcd  (** [Etcd]: Etcd Services *)

| Fs  (** [Fs]: POSIX alike file system. *)

| Ftp  (** [Ftp]: FTP backend. *)

| Gcs  (** [Gcs]: Google Cloud Storage backend. *)

| Ghac  (** [Ghac]: GitHub Action Cache services. *)

| Hdfs  (** [Hdfs]: Hadoop Distributed File System. *)

| Http  (** [Http]: HTTP backend. *)

| Ipfs  (** [Ipfs]: IPFS HTTP Gateway *)

| Ipmfs  (** [Ipmfs]: IPFS mutable file system *)

| Memcached  (** [Memcached]: Memcached service support. *)

| Memory  (** [Memory]: In memory backend support. *)

| MiniMoka  (** [MiniMoka]: Mini Moka backend support. *)

| Moka  (** [Moka]: moka backend support. *)

| Obs  (** [Obs]: Huawei Cloud OBS services. *)

| Onedrive  (** [Onedrive]: Microsoft OneDrive services. *)

| Gdrive  (** [Gdrive]: GoogleDrive services. *)

| Dropbox  (** [Dropbox]: Dropbox services. *)

| Oss  (** [Oss]: Aliyun Object Storage Services *)

| Persy  (** [Persy]: persy backend support. *)

| Redis  (** [Redis]: Redis services *)

| Rocksdb  (** [Rocksdb]: RocksDB services *)

| S3  (** [S3]: AWS S3 alike services. *)

| Sftp  (** [Sftp]: SFTP services *)

| Sled  (** [Sled]: Sled services *)

| Supabase  (** [Supabase]: Supabase storage service *)

| VercelArtifacts
    (** [VercelArtifacts]: Vercel Artifacts service, as known as Vercel Remote Caching. *)

| Wasabi  (** [Wasabi]: Wasabi service *)

| Webdav  (** [Webdav]: WebDAV support. *)

| Webhdfs  (** [Webhdfs]: WebHDFS RESTful API Services *)

| Redb  (** [Redb]: Redb Services *)

| Tikv  (** [Tikv]: Tikv Services *)"
)]
pub enum Scheme {
    Azblob,
    Azdfs,
    Cacache,
    Cos,
    Dashmap,
    Etcd,
    Fs,
    Ftp,
    Gcs,
    Ghac,
    Hdfs,
    Http,
    Ipfs,
    Ipmfs,
    Memcached,
    Memory,
    MiniMoka,
    Moka,
    Obs,
    Onedrive,
    Gdrive,
    Dropbox,
    Oss,
    Persy,
    Redis,
    Rocksdb,
    S3,
    Sftp,
    Sled,
    Supabase,
    VercelArtifacts,
    Wasabi,
    Webdav,
    Webhdfs,
    Redb,
    Tikv,
}

impl From<Scheme> for od::Scheme {
    fn from(value: Scheme) -> Self {
        match value {
            Scheme::Azblob => od::Scheme::Azblob,
            Scheme::Azdfs => od::Scheme::Azdfs,
            Scheme::Cacache => od::Scheme::Cacache,
            Scheme::Cos => od::Scheme::Cos,
            Scheme::Dashmap => od::Scheme::Dashmap,
            Scheme::Etcd => od::Scheme::Etcd,
            Scheme::Fs => od::Scheme::Fs,
            Scheme::Ftp => od::Scheme::Ftp,
            Scheme::Gcs => od::Scheme::Gcs,
            Scheme::Ghac => od::Scheme::Ghac,
            Scheme::Hdfs => od::Scheme::Hdfs,
            Scheme::Http => od::Scheme::Http,
            Scheme::Ipfs => od::Scheme::Ipfs,
            Scheme::Ipmfs => od::Scheme::Ipmfs,
            Scheme::Memcached => od::Scheme::Memcached,
            Scheme::Memory => od::Scheme::Memory,
            Scheme::MiniMoka => od::Scheme::MiniMoka,
            Scheme::Moka => od::Scheme::Moka,
            Scheme::Obs => od::Scheme::Obs,
            Scheme::Onedrive => od::Scheme::Onedrive,
            Scheme::Gdrive => od::Scheme::Gdrive,
            Scheme::Dropbox => od::Scheme::Dropbox,
            Scheme::Oss => od::Scheme::Oss,
            Scheme::Persy => od::Scheme::Persy,
            Scheme::Redis => od::Scheme::Redis,
            Scheme::Rocksdb => od::Scheme::Rocksdb,
            Scheme::S3 => od::Scheme::S3,
            Scheme::Sftp => od::Scheme::Sftp,
            Scheme::Sled => od::Scheme::Sled,
            Scheme::Supabase => od::Scheme::Supabase,
            Scheme::VercelArtifacts => od::Scheme::VercelArtifacts,
            Scheme::Wasabi => od::Scheme::Wasabi,
            Scheme::Webdav => od::Scheme::Webdav,
            Scheme::Webhdfs => od::Scheme::Webhdfs,
            Scheme::Redb => od::Scheme::Redb,
            Scheme::Tikv => od::Scheme::Tikv,
        }
    }
}
