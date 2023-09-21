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

package org.apache.opendal.enums;

public enum Schema {
    Atomicserver("atomicserver"),

    Azblob("azblob"),

    Azdls("azdls"),

    Cacache("cacache"),

    Cos("cos"),

    Dashmap("dashmap"),

    Etcd("etcd"),

    Foundationdb("foundationdb"),

    Fs("fs"),

    Ftp("ftp"),

    Gcs("gcs"),

    Ghac("ghac"),

    Hdfs("hdfs"),

    Http("http"),

    Ipfs("ipfs"),

    Ipmfs("ipmfs"),

    Memcached("memcached"),

    Memory("memory"),

    MiniMoka("minimoka"),

    Moka("moka"),

    Obs("obs"),

    Onedrive("onedrive"),

    Gdrive("gdrive"),

    Dropbox("dropbox"),

    Oss("oss"),

    Persy("persy"),

    Redis("redis"),

    Postgresql("postgresql"),

    Rocksdb("rocksdb"),

    S3("s3"),

    Sftp("sftp"),

    Sled("sled"),

    Supabase("supabase"),

    VercelArtifacts("vercel-artifacts"),

    Wasabi("wasabi"),

    Webdav("webdav"),

    Webhdfs("webhdfs"),

    Redb("redb"),

    Tikv("tikv"),
    ;

    private final String servicesName;

    private Schema(String name) {
        this.servicesName = name;
    }

    public String getServicesName() {
        return servicesName;
    }
}
