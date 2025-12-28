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

use std::collections::HashSet;

use jni::JNIEnv;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::sys::jobjectArray;
use jni::sys::jsize;

use crate::Result;
use crate::convert::string_to_jstring;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OpenDAL_loadEnabledServices(
    mut env: JNIEnv,
    _: JClass,
) -> jobjectArray {
    intern_load_enabled_services(&mut env).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_load_enabled_services(env: &mut JNIEnv) -> Result<jobjectArray> {
    let services = HashSet::from([
        #[cfg(feature = "services-aliyun-drive")]
        opendal::services::ALIYUN_DRIVE_SCHEME,
        #[cfg(feature = "services-alluxio")]
        opendal::services::ALLUXIO_SCHEME,
        #[cfg(feature = "services-azblob")]
        opendal::services::AZBLOB_SCHEME,
        #[cfg(feature = "services-azdls")]
        opendal::services::AZDLS_SCHEME,
        #[cfg(feature = "services-azfile")]
        opendal::services::AZFILE_SCHEME,
        #[cfg(feature = "services-b2")]
        opendal::services::B2_SCHEME,
        #[cfg(feature = "services-cacache")]
        opendal::services::CACACHE_SCHEME,
        #[cfg(feature = "services-cloudflare-kv")]
        opendal::services::CLOUDFLARE_KV_SCHEME,
        #[cfg(feature = "services-compfs")]
        opendal::services::COMPFS_SCHEME,
        #[cfg(feature = "services-cos")]
        opendal::services::COS_SCHEME,
        #[cfg(feature = "services-dashmap")]
        opendal::services::DASHMAP_SCHEME,
        #[cfg(feature = "services-dbfs")]
        opendal::services::DBFS_SCHEME,
        #[cfg(feature = "services-dropbox")]
        opendal::services::DROPBOX_SCHEME,
        #[cfg(feature = "services-etcd")]
        opendal::services::ETCD_SCHEME,
        #[cfg(feature = "services-foundationdb")]
        opendal::services::FOUNDATIONDB_SCHEME,
        #[cfg(feature = "services-fs")]
        opendal::services::FS_SCHEME,
        #[cfg(feature = "services-ftp")]
        opendal::services::FTP_SCHEME,
        #[cfg(feature = "services-gcs")]
        opendal::services::GCS_SCHEME,
        #[cfg(feature = "services-gdrive")]
        opendal::services::GDRIVE_SCHEME,
        #[cfg(feature = "services-ghac")]
        opendal::services::GHAC_SCHEME,
        #[cfg(feature = "services-github")]
        opendal::services::GITHUB_SCHEME,
        #[cfg(feature = "services-gridfs")]
        opendal::services::GRIDFS_SCHEME,
        #[cfg(feature = "services-hdfs")]
        opendal::services::HDFS_SCHEME,
        #[cfg(feature = "services-hdfs-native")]
        opendal::services::HDFS_NATIVE_SCHEME,
        #[cfg(feature = "services-http")]
        opendal::services::HTTP_SCHEME,
        #[cfg(feature = "services-huggingface")]
        opendal::services::HUGGINGFACE_SCHEME,
        #[cfg(feature = "services-ipfs")]
        opendal::services::IPFS_SCHEME,
        #[cfg(feature = "services-ipmfs")]
        opendal::services::IPMFS_SCHEME,
        #[cfg(feature = "services-koofr")]
        opendal::services::KOOFR_SCHEME,
        #[cfg(feature = "services-lakefs")]
        opendal::services::LAKEFS_SCHEME,
        #[cfg(feature = "services-memcached")]
        opendal::services::MEMCACHED_SCHEME,
        #[cfg(feature = "services-memory")]
        opendal::services::MEMORY_SCHEME,
        #[cfg(feature = "services-mini-moka")]
        opendal::services::MINI_MOKA_SCHEME,
        #[cfg(feature = "services-moka")]
        opendal::services::MOKA_SCHEME,
        #[cfg(feature = "services-mongodb")]
        opendal::services::MONGODB_SCHEME,
        #[cfg(feature = "services-monoiofs")]
        opendal::services::MONOIOFS_SCHEME,
        #[cfg(feature = "services-mysql")]
        opendal::services::MYSQL_SCHEME,
        #[cfg(feature = "services-obs")]
        opendal::services::OBS_SCHEME,
        #[cfg(feature = "services-onedrive")]
        opendal::services::ONEDRIVE_SCHEME,
        #[cfg(feature = "services-oss")]
        opendal::services::OSS_SCHEME,
        #[cfg(feature = "services-pcloud")]
        opendal::services::PCLOUD_SCHEME,
        #[cfg(feature = "services-persy")]
        opendal::services::PERSY_SCHEME,
        #[cfg(feature = "services-postgresql")]
        opendal::services::POSTGRESQL_SCHEME,
        #[cfg(feature = "services-redb")]
        opendal::services::REDB_SCHEME,
        #[cfg(feature = "services-redis")]
        opendal::services::REDIS_SCHEME,
        #[cfg(feature = "services-rocksdb")]
        opendal::services::ROCKSDB_SCHEME,
        #[cfg(feature = "services-s3")]
        opendal::services::S3_SCHEME,
        #[cfg(feature = "services-seafile")]
        opendal::services::SEAFILE_SCHEME,
        #[cfg(feature = "services-sftp")]
        opendal::services::SFTP_SCHEME,
        #[cfg(feature = "services-sled")]
        opendal::services::SLED_SCHEME,
        #[cfg(feature = "services-sqlite")]
        opendal::services::SQLITE_SCHEME,
        #[cfg(feature = "services-surrealdb")]
        opendal::services::SURREALDB_SCHEME,
        #[cfg(feature = "services-swift")]
        opendal::services::SWIFT_SCHEME,
        #[cfg(feature = "services-tikv")]
        opendal::services::TIKV_SCHEME,
        #[cfg(feature = "services-upyun")]
        opendal::services::UPYUN_SCHEME,
        #[cfg(feature = "services-vercel-artifacts")]
        opendal::services::VERCEL_ARTIFACTS_SCHEME,
        #[cfg(feature = "services-vercel-blob")]
        opendal::services::VERCEL_BLOB_SCHEME,
        #[cfg(feature = "services-webdav")]
        opendal::services::WEBDAV_SCHEME,
        #[cfg(feature = "services-webhdfs")]
        opendal::services::WEBHDFS_SCHEME,
        #[cfg(feature = "services-yandex-disk")]
        opendal::services::YANDEX_DISK_SCHEME,
    ]);

    let res = env.new_object_array(services.len() as jsize, "java/lang/String", JObject::null())?;

    for (idx, service) in services.into_iter().enumerate() {
        let srv = string_to_jstring(env, Some(service))?;
        env.set_object_array_element(&res, idx as jsize, srv)?;
    }

    Ok(res.into_raw())
}
