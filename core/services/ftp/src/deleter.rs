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

use std::sync::Arc;

use suppaftp::FtpError;
use suppaftp::Status;
use suppaftp::list::File;
use suppaftp::tokio::AsyncRustlsFtpStream;
use suppaftp::types::Response;

use super::core::FtpCore;
use super::core::format_ftp_error;
use super::lister::parse_list_line;
use opendal_core::raw::*;
use opendal_core::*;

pub struct FtpDeleter {
    core: Arc<FtpCore>,
}

impl FtpDeleter {
    pub fn new(core: Arc<FtpCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for FtpDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let mut ftp_stream = self.core.ftp_connect(Operation::Delete).await?;

        let result = if path.ends_with('/') {
            ftp_stream.rmdir(&path).await
        } else {
            ftp_stream.rm(&path).await
        };

        match result {
            Ok(_) => Ok(()),
            Err(
                err @ FtpError::UnexpectedResponse(Response {
                    status: Status::FileUnavailable,
                    ..
                }),
            ) => verify_deleted_after_file_unavailable(&mut ftp_stream, &path, err).await,
            Err(e) => Err(format_ftp_error(e)),
        }
    }
}

async fn verify_deleted_after_file_unavailable(
    ftp_stream: &mut AsyncRustlsFtpStream,
    path: &str,
    delete_err: FtpError,
) -> Result<()> {
    let basename = get_basename(path).trim_end_matches('/');
    if basename.is_empty() {
        return Err(
            Error::new(ErrorKind::Unexpected, "failed to remove directory").set_source(delete_err),
        );
    }

    let parent = get_parent(path);
    let parent_arg = if parent == "/" { None } else { Some(parent) };

    match ftp_stream.list(parent_arg).await {
        Ok(lines) => {
            if let Some(entry) = lines
                .into_iter()
                .filter_map(|line| parse_list_line(line.as_str()).ok().flatten())
                .find(|f| f.name() == basename)
            {
                return Err(delete_existing_entry_error(path, &entry, delete_err));
            }
        }
        Err(FtpError::UnexpectedResponse(Response {
            status: Status::FileUnavailable,
            ..
        })) => {
            // Parent directory itself does not exist, so the target does not exist either.
            return Ok(());
        }
        Err(e) => return Err(format_ftp_error(e)),
    }

    // Some FTP servers (e.g., vsftpd without force_dot_files=YES) omit dotfiles
    // from LIST output. Double-check dotfiles so a failed delete on a hidden
    // file or non-empty hidden directory does not silently succeed.
    if basename.starts_with('.') {
        if path.ends_with('/') {
            if let Ok(lines) = ftp_stream.list(Some(path)).await
                && lines
                    .into_iter()
                    .filter_map(|line| parse_list_line(line.as_str()).ok().flatten())
                    .next()
                    .is_some()
            {
                return Err(
                    Error::new(ErrorKind::Unexpected, "failed to remove directory")
                        .set_source(delete_err),
                );
            }
            if ftp_stream.size(path.trim_end_matches('/')).await.is_ok() {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "delete path is a directory path, but the target is a file",
                )
                .set_source(delete_err));
            }
        } else if ftp_stream.size(path).await.is_ok() {
            return Err(
                Error::new(ErrorKind::Unexpected, "failed to remove file").set_source(delete_err)
            );
        }
    }

    Ok(())
}

fn delete_existing_entry_error(path: &str, entry: &File, delete_err: FtpError) -> Error {
    if path.ends_with('/') && entry.is_file() {
        Error::new(
            ErrorKind::NotADirectory,
            "delete path is a directory path, but the target is a file",
        )
        .set_source(delete_err)
    } else if !path.ends_with('/') && entry.is_directory() {
        Error::new(
            ErrorKind::IsADirectory,
            "delete path is a file path, but the target is a directory",
        )
        .set_source(delete_err)
    } else {
        let msg = if path.ends_with('/') {
            "failed to remove directory"
        } else {
            "failed to remove file"
        };
        Error::new(ErrorKind::Unexpected, msg).set_source(delete_err)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn file_unavailable_err() -> FtpError {
        FtpError::UnexpectedResponse(Response::new(
            Status::FileUnavailable,
            b"550 operation failed\r\n".to_vec(),
        ))
    }

    #[test]
    fn test_delete_existing_entry_error_kinds() {
        let dir = File::from_str("drwxr-xr-x 2 user group 4096 Nov  5  2018 target").unwrap();
        let file = File::from_str("-rw-r--r-- 1 user group 10 Nov  5  2018 target").unwrap();

        assert_eq!(
            delete_existing_entry_error("target/", &dir, file_unavailable_err()).kind(),
            ErrorKind::Unexpected
        );
        assert_eq!(
            delete_existing_entry_error("target/", &file, file_unavailable_err()).kind(),
            ErrorKind::NotADirectory
        );
        assert_eq!(
            delete_existing_entry_error("target", &dir, file_unavailable_err()).kind(),
            ErrorKind::IsADirectory
        );
        assert_eq!(
            delete_existing_entry_error("target", &file, file_unavailable_err()).kind(),
            ErrorKind::Unexpected
        );
    }
}
