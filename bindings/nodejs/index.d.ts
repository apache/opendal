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

/// <reference types="node" />

interface SchemeOptions {
  // Azure Storage Blob services support.
  azblob: {
    // The work dir for backend.
    root?: string
    // The container name for backend.
    container: string
    // The endpoint for backend.
    endpoint?: string
    // The account_name for backend.
    accountName?: string
    // The account_key for backend.
    accountKey?: string
  }
  // Azure Data Lake Storage Gen2 Support.
  //
  // As known as `abfs`, `azdfs` or `azdls`.
  //
  // This service will visist the ABFS URI supported by [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
  azdfs: {
    // The work dir for backend.
    root?: string
    // The filesystem name for backend.
    filesystem: string
    // The endpoint for backend.
    endpoint?: string
    // The account_name for backend.
    accountName?: string
    // The account_key for backend.
    accountKey?: string
  }
  // POSIX file system support.
  fs: {
    // The work dir for backend.
    root?: string
  }
  // Google Cloud Storage service.
  gcs: {
    // The work directory for backend
    root?: string
    // The container name for backend
    bucket: string
    // Customizable endpoint setting
    endpoint?: string
    // Credential string for GCS OAuth2
    credentials?: string
  }
  // GitHub Action Cache Services support.
  ghac: {}
  // HTTP Read-only service support like Nginx and Caddy.
  http: {
    // The endpoint for http
    endpoint?: string
    // The work directory for backend
    root?: string
  }
  // IPFS file system support based on [IPFS MFS](https://docs.ipfs.tech/concepts/file-systems/) API.
  ipmfs: {
    // The work directory for backend
    root?: string
    // Customizable endpoint setting
    endpoint?: string
  }
  // In memory service support. (HashMap Based)
  memory: {}
  // Huawei Cloud OBS services support.
  obs: {
    // The work directory for backend
    root?: string
    // The container name for backend
    bucket?: string
    // Customizable endpoint setting
    endpoint?: string
    // The access_key_id for backend.
    accessKeyId?: string
    // The secret_access_key for backend.
    secretAccessKey?: string
  },
  // Aliyun Object Storage Service (OSS) support
  oss: {
    // The work dir for backend.
    root?: string
    // The container name for backend.
    bucket: string
    // The endpoint for backend.
    endpoint?: string
    // The endpoint for presign.
    presign_endpoint?: string
    // The access_key_id for backend.
    access_key_id?: string
    // The access_key_secret for backend.
    access_key_secret?: string
    // The role of backend.
    role_arn?: string
    // The oidc_token for backend.
    oidc_token?: string
    // The backend access OSS in anonymous way.
    allow_anonymous: boolean
  }
  // [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
  webdav: {
    //  The endpoint for webdav
    endpoint?: string
    // The work directory for backend
    root?: string
  }
  // WebDAV backend support.
  webhdfs: {
    // The endpoint for webdav
    endpoint?: string
    //  The work directory for backend
    root?: string
  }
  // Aws S3 and compatible services (including minio, digitalocean space and so on) support
  s3: {
    // The work dir for backend.
    root?: string
    // The container name for backend.
    bucket: string
    // The endpoint for backend.
    endpoint?: string
    // The region for backend.
    region?: string
    // The access_key_id for backend.
    accessKeyId?: string
    // The secret_access_key for backend.
    secretAccessKey?: string
    // The security_token for backend.
    securityToken?: string
    // The server_side_encryption for backend.
    serverSideEncryption?: string
    // The server_side_encryption_aws_kms_key_id for backend.
    serverSideEncryptionAwsKmsKeyId?: string
    // The server_side_encryption_customer_algorithm for backend.
    serverSideEncryptionCustomerAlgorithm?: string
    // The server_side_encryption_customer_key for backend.
    serverSideEncryptionCustomerKey?: string
    // The server_side_encryption_customer_key_md5 for backend.
    serverSideEncryptionCustomerKeyMd5?: string
    // Disable aws config load from env
    disableConfigLoad?: string
    // Enable virtual host style.
    enableVirtualHostStyle?: string
  }
}

// The currently available service types.
type Scheme = keyof SchemeOptions

export class Operator<T extends Scheme> {
  constructor(scheme: T, options?: SchemeOptions[T] | undefined | null)
  stat(path: string): Promise<Metadata>
  statSync(path: string): Metadata
  createDir(path: string): Promise<void>
  createDirSync(path: string): void
  write(path: string, content: Buffer | string): Promise<void>
  writeSync(path: string, content: Buffer | string): void
  read(path: string): Promise<Buffer>
  readSync(path: string): Buffer
  scan(path: string): Promise<Lister>
  scanSync(path: string): BlockingLister
  delete(path: string): Promise<void>
  deleteSync(path: string): void
  list(path: string): Promise<Lister>
  listSync(path: string): BlockingLister
}
export class Entry {
  path(): string
}
export class Metadata {
  /** Returns true if the <op.stat> object describes a file system directory. */
  isDirectory(): boolean
  /** Returns true if the <op.stat> object describes a regular file. */
  isFile(): boolean
  /** Content-Disposition of this object */
  get contentDisposition(): string | null
  /** Content Length of this object */
  get contentLength(): bigint | null
  /** Content MD5 of this object. */
  get contentMd5(): string | null
  /** Content Type of this object. */
  get contentType(): string | null
  /** ETag of this object. */
  get etag(): string | null
  /** Last Modified of this object.(UTC) */
  get lastModified(): string | null
}
export class Lister {
  next(): Promise<Entry | null>
}
export class BlockingLister {
  next(): Entry | null
}
