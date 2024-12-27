/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Operator } from './generated'

export interface Aliyun_drive_Config {
  root?: string
  access_token?: string
  client_id?: string
  client_secret?: string
  refresh_token?: string
  drive_type: string
}

export interface Alluxio_Config {
  root?: string
  endpoint?: string
}

export interface Atomicserver_Config {
  root?: string
  endpoint?: string
  private_key?: string
  public_key?: string
  parent_resource_id?: string
}

export interface Azblob_Config {
  root?: string
  container: string
  endpoint?: string
  account_name?: string
  account_key?: string
  encryption_key?: string
  encryption_key_sha256?: string
  encryption_algorithm?: string
  sas_token?: string
  batch_max_operations?: string
}

export interface Azdls_Config {
  root?: string
  filesystem: string
  endpoint?: string
  account_name?: string
  account_key?: string
}

export interface Azfile_Config {
  root?: string
  endpoint?: string
  share_name: string
  account_name?: string
  account_key?: string
  sas_token?: string
}

export interface B2_Config {
  root?: string
  application_key_id?: string
  application_key?: string
  bucket: string
  bucket_id: string
}

export interface Cacache_Config {
  datadir?: string
}

export interface Chainsafe_Config {
  root?: string
  api_key?: string
  bucket_id: string
}

export interface Cloudflare_kv_Config {
  token?: string
  account_id?: string
  namespace_id?: string
  root?: string
}

export interface Compfs_Config {
  root?: string
}

export interface Cos_Config {
  root?: string
  endpoint?: string
  secret_id?: string
  secret_key?: string
  bucket?: string
  disable_config_load?: string
}

export interface D1_Config {
  token?: string
  account_id?: string
  database_id?: string
  root?: string
  table?: string
  key_field?: string
  value_field?: string
}

export interface Dashmap_Config {
  root?: string
}

export interface Dbfs_Config {
  root?: string
  endpoint?: string
  token?: string
}

export interface Dropbox_Config {
  root?: string
  access_token?: string
  refresh_token?: string
  client_id?: string
  client_secret?: string
}

export interface Fs_Config {
  root?: string
  atomic_write_dir?: string
}

export interface Gcs_Config {
  root?: string
  bucket: string
  endpoint?: string
  scope?: string
  service_account?: string
  credential?: string
  credential_path?: string
  predefined_acl?: string
  default_storage_class?: string
  allow_anonymous?: string
  disable_vm_metadata?: string
  disable_config_load?: string
  token?: string
}

export interface Gdrive_Config {
  root?: string
  access_token?: string
  refresh_token?: string
  client_id?: string
  client_secret?: string
}

export interface Ghac_Config {
  root?: string
  version?: string
  endpoint?: string
  runtime_token?: string
}

export interface Github_Config {
  root?: string
  token?: string
  owner: string
  repo: string
}

export interface Gridfs_Config {
  connection_string?: string
  database?: string
  bucket?: string
  chunk_size?: string
  root?: string
}

export interface Hdfs_native_Config {
  root?: string
  url?: string
  enable_append?: string
}

export interface Http_Config {
  endpoint?: string
  username?: string
  password?: string
  token?: string
  root?: string
}

export interface Huggingface_Config {
  repo_type?: string
  repo_id?: string
  revision?: string
  root?: string
  token?: string
}

export interface Icloud_Config {
  root?: string
  apple_id?: string
  password?: string
  trust_token?: string
  ds_web_auth_token?: string
  is_china_mainland?: string
}

export interface Ipfs_Config {
  endpoint?: string
  root?: string
}

export interface Ipmfs_Config {
  root?: string
  endpoint?: string
}

export interface Koofr_Config {
  root?: string
  endpoint: string
  email: string
  password?: string
}

export interface Lakefs_Config {
  endpoint?: string
  username?: string
  password?: string
  root?: string
  repository?: string
  branch?: string
}

export interface Libsql_Config {
  connection_string?: string
  auth_token?: string
  table?: string
  key_field?: string
  value_field?: string
  root?: string
}

export interface Memcached_Config {
  endpoint?: string
  root?: string
  username?: string
  password?: string
  default_ttl?: string
}

export interface Memory_Config {
  root?: string
}

export interface Mini_moka_Config {
  max_capacity?: string
  time_to_live?: string
  time_to_idle?: string
  root?: string
}

export interface Moka_Config {
  name?: string
  max_capacity?: string
  time_to_live?: string
  time_to_idle?: string
  num_segments?: string
  root?: string
}

export interface Mongodb_Config {
  connection_string?: string
  database?: string
  collection?: string
  root?: string
  key_field?: string
  value_field?: string
}

export interface Monoiofs_Config {
  root?: string
}

export interface Mysql_Config {
  connection_string?: string
  table?: string
  key_field?: string
  value_field?: string
  root?: string
}

export interface Nebula_graph_Config {
  host?: string
  port?: string
  username?: string
  password?: string
  space?: string
  tag?: string
  key_field?: string
  value_field?: string
  root?: string
}

export interface Obs_Config {
  root?: string
  endpoint?: string
  access_key_id?: string
  secret_access_key?: string
  bucket?: string
}

export interface Onedrive_Config {
  access_token?: string
  root?: string
}

export interface Oss_Config {
  root?: string
  endpoint?: string
  presign_endpoint?: string
  bucket: string
  server_side_encryption?: string
  server_side_encryption_key_id?: string
  allow_anonymous?: string
  access_key_id?: string
  access_key_secret?: string

  /**
   * @deprecated: Please use `delete_max_size` instead of `batch_max_operations`
   */
  batch_max_operations?: string
  delete_max_size?: string
  role_arn?: string
  role_session_name?: string
  oidc_provider_arn?: string
  oidc_token_file?: string
  sts_endpoint?: string
}

export interface Pcloud_Config {
  root?: string
  endpoint: string
  username?: string
  password?: string
}

export interface Persy_Config {
  datafile?: string
  segment?: string
  index?: string
}

export interface Postgresql_Config {
  root?: string
  connection_string?: string
  table?: string
  key_field?: string
  value_field?: string
}

export interface Redb_Config {
  datadir?: string
  root?: string
  table?: string
}

export interface Redis_Config {
  endpoint?: string
  cluster_endpoints?: string
  username?: string
  password?: string
  root?: string
  db: string
  default_ttl?: string
}

export interface S3_Config {
  root?: string
  bucket: string
  enable_versioning?: string
  endpoint?: string
  region?: string
  access_key_id?: string
  secret_access_key?: string
  session_token?: string
  role_arn?: string
  external_id?: string
  role_session_name?: string
  disable_config_load?: string
  disable_ec2_metadata?: string
  allow_anonymous?: string
  server_side_encryption?: string
  server_side_encryption_aws_kms_key_id?: string
  server_side_encryption_customer_algorithm?: string
  server_side_encryption_customer_key?: string
  server_side_encryption_customer_key_md5?: string
  default_storage_class?: string
  enable_virtual_host_style?: string

  /**
   * @deprecated: Please use `delete_max_size` instead of `batch_max_operations`
   */
  batch_max_operations?: string
  delete_max_size?: string
  disable_stat_with_override?: string
  checksum_algorithm?: string
  disable_write_with_if_match?: string
}

export interface Seafile_Config {
  root?: string
  endpoint?: string
  username?: string
  password?: string
  repo_name: string
}

export interface Sftp_Config {
  endpoint?: string
  root?: string
  user?: string
  key?: string
  known_hosts_strategy?: string
  enable_copy?: string
}

export interface Sled_Config {
  datadir?: string
  root?: string
  tree?: string
}

export interface Sqlite_Config {
  connection_string?: string
  table?: string
  key_field?: string
  value_field?: string
  root?: string
}

export interface Supabase_Config {
  root?: string
  bucket: string
  endpoint?: string
  key?: string
}

export interface Surrealdb_Config {
  connection_string?: string
  username?: string
  password?: string
  namespace?: string
  database?: string
  table?: string
  key_field?: string
  value_field?: string
  root?: string
}

export interface Swift_Config {
  endpoint?: string
  container?: string
  root?: string
  token?: string
}

export interface Upyun_Config {
  root?: string
  bucket: string
  operator?: string
  password?: string
}

export interface Vercel_artifacts_Config {
  access_token?: string
}

export interface Vercel_blob_Config {
  root?: string
  token?: string
}

export interface Webdav_Config {
  endpoint?: string
  username?: string
  password?: string
  token?: string
  root?: string
  disable_copy?: string
}

export interface Webhdfs_Config {
  root?: string
  endpoint?: string
  delegation?: string
  disable_list_batch?: string
  atomic_write_dir?: string
}

export interface Yandex_disk_Config {
  root?: string
  access_token: string
}

export function create_operator(scheme: 'aliyun_drive', options?: Aliyun_drive_Config | undefined | null): Operator
export function create_operator(scheme: 'alluxio', options?: Alluxio_Config | undefined | null): Operator
export function create_operator(scheme: 'atomicserver', options?: Atomicserver_Config | undefined | null): Operator
export function create_operator(scheme: 'azblob', options?: Azblob_Config | undefined | null): Operator
export function create_operator(scheme: 'azdls', options?: Azdls_Config | undefined | null): Operator
export function create_operator(scheme: 'azfile', options?: Azfile_Config | undefined | null): Operator
export function create_operator(scheme: 'b2', options?: B2_Config | undefined | null): Operator
export function create_operator(scheme: 'cacache', options?: Cacache_Config | undefined | null): Operator
export function create_operator(scheme: 'chainsafe', options?: Chainsafe_Config | undefined | null): Operator
export function create_operator(scheme: 'cloudflare_kv', options?: Cloudflare_kv_Config | undefined | null): Operator
export function create_operator(scheme: 'compfs', options?: Compfs_Config | undefined | null): Operator
export function create_operator(scheme: 'cos', options?: Cos_Config | undefined | null): Operator
export function create_operator(scheme: 'd1', options?: D1_Config | undefined | null): Operator
export function create_operator(scheme: 'dashmap', options?: Dashmap_Config | undefined | null): Operator
export function create_operator(scheme: 'dbfs', options?: Dbfs_Config | undefined | null): Operator
export function create_operator(scheme: 'dropbox', options?: Dropbox_Config | undefined | null): Operator
export function create_operator(scheme: 'fs', options?: Fs_Config | undefined | null): Operator
export function create_operator(scheme: 'gcs', options?: Gcs_Config | undefined | null): Operator
export function create_operator(scheme: 'gdrive', options?: Gdrive_Config | undefined | null): Operator
export function create_operator(scheme: 'ghac', options?: Ghac_Config | undefined | null): Operator
export function create_operator(scheme: 'github', options?: Github_Config | undefined | null): Operator
export function create_operator(scheme: 'gridfs', options?: Gridfs_Config | undefined | null): Operator
export function create_operator(scheme: 'hdfs_native', options?: Hdfs_native_Config | undefined | null): Operator
export function create_operator(scheme: 'http', options?: Http_Config | undefined | null): Operator
export function create_operator(scheme: 'huggingface', options?: Huggingface_Config | undefined | null): Operator
export function create_operator(scheme: 'icloud', options?: Icloud_Config | undefined | null): Operator
export function create_operator(scheme: 'ipfs', options?: Ipfs_Config | undefined | null): Operator
export function create_operator(scheme: 'ipmfs', options?: Ipmfs_Config | undefined | null): Operator
export function create_operator(scheme: 'koofr', options?: Koofr_Config | undefined | null): Operator
export function create_operator(scheme: 'lakefs', options?: Lakefs_Config | undefined | null): Operator
export function create_operator(scheme: 'libsql', options?: Libsql_Config | undefined | null): Operator
export function create_operator(scheme: 'memcached', options?: Memcached_Config | undefined | null): Operator
export function create_operator(scheme: 'memory', options?: Memory_Config | undefined | null): Operator
export function create_operator(scheme: 'mini_moka', options?: Mini_moka_Config | undefined | null): Operator
export function create_operator(scheme: 'moka', options?: Moka_Config | undefined | null): Operator
export function create_operator(scheme: 'mongodb', options?: Mongodb_Config | undefined | null): Operator
export function create_operator(scheme: 'monoiofs', options?: Monoiofs_Config | undefined | null): Operator
export function create_operator(scheme: 'mysql', options?: Mysql_Config | undefined | null): Operator
export function create_operator(scheme: 'nebula_graph', options?: Nebula_graph_Config | undefined | null): Operator
export function create_operator(scheme: 'obs', options?: Obs_Config | undefined | null): Operator
export function create_operator(scheme: 'onedrive', options?: Onedrive_Config | undefined | null): Operator
export function create_operator(scheme: 'oss', options?: Oss_Config | undefined | null): Operator
export function create_operator(scheme: 'pcloud', options?: Pcloud_Config | undefined | null): Operator
export function create_operator(scheme: 'persy', options?: Persy_Config | undefined | null): Operator
export function create_operator(scheme: 'postgresql', options?: Postgresql_Config | undefined | null): Operator
export function create_operator(scheme: 'redb', options?: Redb_Config | undefined | null): Operator
export function create_operator(scheme: 'redis', options?: Redis_Config | undefined | null): Operator
export function create_operator(scheme: 's3', options?: S3_Config | undefined | null): Operator
export function create_operator(scheme: 'seafile', options?: Seafile_Config | undefined | null): Operator
export function create_operator(scheme: 'sftp', options?: Sftp_Config | undefined | null): Operator
export function create_operator(scheme: 'sled', options?: Sled_Config | undefined | null): Operator
export function create_operator(scheme: 'sqlite', options?: Sqlite_Config | undefined | null): Operator
export function create_operator(scheme: 'supabase', options?: Supabase_Config | undefined | null): Operator
export function create_operator(scheme: 'surrealdb', options?: Surrealdb_Config | undefined | null): Operator
export function create_operator(scheme: 'swift', options?: Swift_Config | undefined | null): Operator
export function create_operator(scheme: 'upyun', options?: Upyun_Config | undefined | null): Operator
export function create_operator(
  scheme: 'vercel_artifacts',
  options?: Vercel_artifacts_Config | undefined | null,
): Operator
export function create_operator(scheme: 'vercel_blob', options?: Vercel_blob_Config | undefined | null): Operator
export function create_operator(scheme: 'webdav', options?: Webdav_Config | undefined | null): Operator
export function create_operator(scheme: 'webhdfs', options?: Webhdfs_Config | undefined | null): Operator
export function create_operator(scheme: 'yandex_disk', options?: Yandex_disk_Config | undefined | null): Operator

export function create_operator(scheme: string, options?: Record<string, string> | undefined | null): Operator
