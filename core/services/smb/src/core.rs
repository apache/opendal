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

use std::fmt::Debug;
use std::sync::Arc;

use fastpool::{ManageObject, ObjectStatus, bounded};
use log::debug;
use smb::{
    Client, ClientConfig, CreateDisposition, CreateOptions, DirAccessMask, FileAccessMask,
    FileAttributes, FileCreateArgs, GetLen, Resource, UncPath,
};

use super::error::is_already_exists;
use super::error::parse_smb_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SmbCore {
    pub info: Arc<AccessorInfo>,
    pub endpoint: String,
    pub share: String,
    pub root: String,
    client: Option<Arc<bounded::Pool<Manager>>>,
}

impl Debug for SmbCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmbCore")
            .field("endpoint", &self.endpoint)
            .field("share", &self.share)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl SmbCore {
    pub fn new(
        info: Arc<AccessorInfo>,
        endpoint: String,
        share: String,
        root: String,
        user: Option<String>,
        password: Option<String>,
    ) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoint: endpoint.clone(),
                share: share.clone(),
                root: root.clone(),
                user: user.unwrap_or_default(),
                password: password.unwrap_or_default(),
            },
        );

        Self {
            info,
            endpoint,
            share,
            root,
            client: Some(client),
        }
    }

    pub async fn connect(&self) -> Result<bounded::Object<Manager>> {
        let fut = self
            .client
            .as_ref()
            .expect("smb connection pool must exist")
            .get();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Err(Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary())
            }
            result = fut => match result {
                Ok(client) => Ok(client),
                Err(err) => Err(err),
            }
        }
    }

    pub fn build_relative_path(&self, path: &str) -> String {
        build_relative_path(&self.root, path)
    }

    pub fn build_unc_path(&self, path: &str) -> Result<UncPath> {
        let path = build_smb_path(path);
        let mut unc = UncPath::new(&self.endpoint)
            .map_err(parse_smb_error)?
            .with_share(&self.share)
            .map_err(parse_smb_error)?;

        if !path.is_empty() {
            unc = unc.with_path(&path);
        }

        Ok(unc)
    }

    pub async fn ensure_dir_exists(&self, client: &Client, path: &str) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }

        let mut current = String::new();
        for component in path.split('/').filter(|v| !v.is_empty()) {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);

            let unc = self.build_unc_path(&current)?;
            let result = client
                .create_file(
                    &unc,
                    &FileCreateArgs::make_create_new(
                        FileAttributes::new().with_directory(true),
                        CreateOptions::new().with_directory_file(true),
                    ),
                )
                .await;

            match result {
                Ok(resource) => close_resource(resource).await?,
                Err(err) if is_already_exists(&err) => {}
                Err(err) => return Err(parse_smb_error(err)),
            }
        }

        Ok(())
    }

    pub async fn stat_path(&self, client: &Client, path: &str) -> Result<Metadata> {
        let unc = self.build_unc_path(path)?;
        let args = FileCreateArgs {
            disposition: CreateDisposition::Open,
            attributes: FileAttributes::new(),
            options: CreateOptions::new(),
            desired_access: FileAccessMask::new()
                .with_file_read_attributes(true)
                .with_synchronize(true),
        };

        let resource = client
            .create_file(&unc, &args)
            .await
            .map_err(parse_smb_error)?;

        match resource {
            Resource::File(file) => {
                let mut meta = Metadata::new(EntryMode::FILE);
                let len = match file.get_len().await {
                    Ok(len) => len,
                    Err(err) => {
                        return Err(close_resource_after_error(
                            Resource::File(file),
                            parse_smb_error(err),
                        )
                        .await);
                    }
                };
                meta.set_content_length(len);
                set_last_modified(&mut meta, file.modified().assume_utc().unix_timestamp());
                file.close().await.map_err(parse_smb_error)?;
                Ok(meta)
            }
            Resource::Directory(dir) => {
                let mut meta = Metadata::new(EntryMode::DIR);
                set_last_modified(&mut meta, dir.modified().assume_utc().unix_timestamp());
                dir.close().await.map_err(parse_smb_error)?;
                Ok(meta)
            }
            other => {
                close_resource(other).await?;
                Ok(Metadata::new(EntryMode::Unknown))
            }
        }
    }

    pub async fn delete_path(&self, client: &Client, path: &str, is_dir: bool) -> Result<()> {
        let unc = self.build_unc_path(path)?;
        let resource = match client
            .create_file(
                &unc,
                &FileCreateArgs::make_open_existing(
                    FileAccessMask::new()
                        .with_delete(true)
                        .with_synchronize(true),
                ),
            )
            .await
        {
            Ok(resource) => resource,
            Err(err) if super::error::is_not_found(&err) => return Ok(()),
            Err(err) => return Err(parse_smb_error(err)),
        };

        match resource {
            Resource::File(file) if !is_dir => {
                if let Err(err) = file
                    .set_info(smb::FileDispositionInformation::default())
                    .await
                {
                    return Err(close_resource_after_error(
                        Resource::File(file),
                        parse_smb_error(err),
                    )
                    .await);
                }
                match file.close().await {
                    Ok(_) => {}
                    Err(err) if super::error::is_not_found(&err) => {}
                    Err(err) => return Err(parse_smb_error(err)),
                }
            }
            Resource::Directory(dir) if is_dir => {
                if let Err(err) = dir
                    .set_info(smb::FileDispositionInformation::default())
                    .await
                {
                    return Err(close_resource_after_error(
                        Resource::Directory(dir),
                        parse_smb_error(err),
                    )
                    .await);
                }
                match dir.close().await {
                    Ok(_) => {}
                    Err(err) if super::error::is_not_found(&err) => {}
                    Err(err) => return Err(parse_smb_error(err)),
                }
            }
            Resource::File(file) => {
                file.close().await.map_err(parse_smb_error)?;
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "delete path expected a directory but found a file",
                ));
            }
            Resource::Directory(dir) => {
                dir.close().await.map_err(parse_smb_error)?;
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "delete path expected a file but found a directory",
                ));
            }
            other => {
                close_resource(other).await?;
            }
        }

        Ok(())
    }
}

impl Drop for SmbCore {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };

        if tokio::runtime::Handle::try_current().is_ok() {
            drop(client);
        } else {
            std::mem::forget(client);
        }
    }
}

pub struct Manager {
    endpoint: String,
    share: String,
    root: String,
    user: String,
    password: String,
}

impl ManageObject for Manager {
    type Object = Client;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        let client = Client::new(ClientConfig::default());
        let mut unc = UncPath::new(&self.endpoint).map_err(parse_smb_error)?;
        unc = unc.with_share(&self.share).map_err(parse_smb_error)?;

        client
            .share_connect(&unc, &self.user, self.password.clone())
            .await
            .map_err(parse_smb_error)?;

        let root = build_relative_path(&self.root, "/");
        if !root.is_empty() {
            let core = SmbCore::new(
                Arc::new(AccessorInfo::default()),
                self.endpoint.clone(),
                self.share.clone(),
                String::new(),
                Some(self.user.clone()),
                Some(self.password.clone()),
            );
            core.ensure_dir_exists(&client, &root).await?;
        }

        Ok(client)
    }

    async fn is_recyclable(
        &self,
        client: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        let unc = UncPath::new(&self.endpoint)
            .map_err(parse_smb_error)?
            .with_share(&self.share)
            .map_err(parse_smb_error)?;

        let resource = client
            .create_file(
                &unc,
                &FileCreateArgs {
                    disposition: CreateDisposition::Open,
                    attributes: FileAttributes::new(),
                    options: CreateOptions::new().with_directory_file(true),
                    desired_access: DirAccessMask::new()
                        .with_list_directory(true)
                        .with_read_attributes(true)
                        .with_synchronize(true)
                        .into(),
                },
            )
            .await
            .map_err(parse_smb_error)?;

        close_resource(resource).await
    }
}

fn build_relative_path(root: &str, path: &str) -> String {
    build_abs_path(root, path)
}

fn build_smb_path(path: &str) -> String {
    path.trim_matches('/').replace('/', "\\")
}

pub(super) async fn close_resource(resource: Resource) -> Result<()> {
    match resource {
        Resource::File(file) => file.close().await.map_err(parse_smb_error),
        Resource::Directory(dir) => dir.close().await.map_err(parse_smb_error),
        Resource::Pipe(pipe) => pipe.close().await.map_err(parse_smb_error),
    }
}

async fn close_resource_after_error(resource: Resource, err: Error) -> Error {
    if let Err(close_err) = close_resource(resource).await {
        debug!("failed to close smb resource after error: {close_err:?}");
    }

    err
}

fn set_last_modified(meta: &mut Metadata, modified: i64) {
    if let Ok(ts) = Timestamp::from_second(modified) {
        meta.set_last_modified(ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_relative_path_keeps_opendal_style_paths() {
        assert_eq!(build_relative_path("/", "/"), "");
        assert_eq!(build_relative_path("/nested/", "/"), "nested/");
        assert_eq!(
            build_relative_path("/nested/", "dir/file"),
            "nested/dir/file"
        );
    }

    #[test]
    fn build_smb_path_converts_only_at_the_edge() {
        assert_eq!(build_smb_path(""), "");
        assert_eq!(build_smb_path("nested/dir/file"), r"nested\dir\file");
        assert_eq!(build_smb_path("nested/dir/"), r"nested\dir");
    }
}
