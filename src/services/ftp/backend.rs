use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::str;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::info;
use suppaftp::list::File;
use suppaftp::native_tls::{TlsConnector, TlsStream};
use suppaftp::{FtpError, FtpResult, FtpStream, Status};
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::dir_stream::ReadDir;
use super::err::new_request_append_error;
use super::err::new_request_connection_error;
use super::err::new_request_list_error;
use super::err::new_request_mdtm_error;
use super::err::new_request_mkdir_error;
use super::err::new_request_put_error;
use super::err::new_request_quit_error;
use super::err::new_request_remove_error;
use super::err::new_request_retr_error;
use super::err::new_request_secure_error;
use super::err::new_request_sign_error;
use super::err::parse_io_error;
use crate::error::other;
use crate::error::BackendError;
use crate::io_util::unshared_reader;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::path;
use crate::Accessor;
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;

// Builder for ftp backend.
#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
    port: Option<String>,
    root: Option<String>,
    credential: Option<(String, String)>,
    tls: Option<bool>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            //.field("stream", &self.stream)
            .finish()
    }
}

impl Builder {
    // set endpoint for ftp backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    // set port for ftp backend.
    pub fn port(&mut self, port: &str) -> &mut Self {
        self.port = if port.is_empty() {
            None
        } else {
            Some(port.to_string())
        };

        self
    }

    // set root path for ftp backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    // set credential for ftp backend.
    pub fn credential(&mut self, credential: (String, String)) -> &mut Self {
        self.credential = if credential.0.is_empty() && credential.1.is_empty() {
            None
        } else {
            Some(credential)
        };

        self
    }

    // set tls for ftp backend.
    pub fn tls(&mut self, tls: bool) -> &mut Self {
        self.tls = Some(tls);
        self
    }

    // Build a ftp backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("ftp backend build started: {:?}", &self);
        let endpoint = match &self.endpoint {
            None => {
                return Err(other(BackendError::new(
                    HashMap::new(),
                    anyhow!("endpoint must be specified"),
                )))
            }
            Some(v) => v,
        };

        let port = match &self.port {
            None => "21".to_string(),
            Some(v) => v.clone(),
        };

        let root = match &self.root {
            // set default path to '/'
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());
                let mut v = v.clone();
                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }
                v
            }
        };

        let credential = match &self.credential {
            None => ("".to_string(), "".to_string()),
            Some(v) => v.clone(),
        };

        let tls = match &self.tls {
            None => false,
            Some(v) => *v,
        };
        /*
        let client = FtpStream{
            reader: None,
            mode: Passive,
            nat_workaround: false,
            welcome_msg: None,
            tls_ctx: None,
            domain: None,
        };
        */

        info!("ftp backend finished: {:?}", &self);
        Ok(Backend {
            endpoint: endpoint.to_string(),
            port,
            root,
            credential,
            tls,
            //stream,
        })
    }
}

#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    port: String,
    root: String,
    credential: (String, String),
    tls: bool,
    // stream: FtpStream,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("credential", &self.credential)
            //.field("stream", &self.stream)
            .finish()
    }
}

impl Backend {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                _ => continue,
            };
        }
        builder.build()
    }

    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        match path.strip_prefix(&self.root) {
            Some(v) => v.to_string(),
            None => unreachable!(
                "invalid path{} that does not start with backend root {}",
                &path, &self.root
            ),
        }
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.to_string();
        }
        format!("{}{}", self.root, path)
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = self.get_abs_path(args.path());

        if args.mode() == ObjectMode::FILE {
            self.ftp_put(&path).await?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            self.ftp_mkdir(&path).await?;

            return Ok(());
        }

        unreachable!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        let reader = self.ftp_get(&path).await?;

        let mut reader = Compat::new(reader);

        if let Some(offset) = args.offset() {
            reader
                .seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(e, "read", args.path()))?;
        }

        let r: BytesReader = match args.size() {
            Some(size) => Box::new(reader.take(size)),
            None => Box::new(reader),
        };

        Ok(r)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = self.get_abs_path(args.path());

        let n = self.ftp_append(&path, unshared_reader(r)).await?;

        Ok(n)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = self.get_abs_path(args.path());

        if path == self.root {
            let mut meta = ObjectMetadata::default();
            meta.set_mode(ObjectMode::DIR);
            return Ok(meta);
        }

        let resp = self.ftp_stat(&path).await?;

        let mut meta = ObjectMetadata::default();

        if resp.is_file() {
            meta.set_mode(ObjectMode::FILE);
        } else if resp.is_directory() {
            meta.set_mode(ObjectMode::DIR);
        } else {
            meta.set_mode(ObjectMode::Unknown);
        }

        meta.set_content_length(resp.size() as u64);

        meta.set_last_modified(OffsetDateTime::from(resp.modified()));

        Ok(meta)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(args.path());

        self.ftp_delete(&path).await?;

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = self.get_abs_path(args.path());

        let rd = self.ftp_list(&path).await?;

        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            path.as_str(),
            rd,
        )))
    }
}

impl Backend {
    pub(crate) fn ftp_connect(&self, url: &String) -> Result<FtpStream> {
        let mut ftp_stream = FtpStream::connect(url)
            .map_err(|e| new_request_connection_error("connection", &url, e))?;

        // switch to secure mode if tls mode is on.
        if self.tls {
            ftp_stream = ftp_stream
                .into_secure(TlsConnector::new().unwrap(), &self.endpoint)
                .map_err(|e| new_request_secure_error("connection", &url, e))?;
        }

        // login if needed
        if !self.credential.0.is_empty() {
            ftp_stream
                .login(&self.credential.0, &self.credential.1)
                .map_err(|e| new_request_sign_error("connection", &url, e))?;
        }

        Ok(ftp_stream)
    }

    pub(crate) async fn ftp_get(&self, path: &str) -> Result<Cursor<Vec<u8>>> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let reader = ftp_stream
            .retr_as_buffer(&url)
            .map_err(|e| new_request_retr_error("get", &url, e))?;

        ftp_stream
            .quit()
            .map_err(|e| new_request_quit_error("get", &url, e))?;

        Ok(reader)
    }

    pub(crate) async fn ftp_put(&self, path: &str) -> Result<u64> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let n = ftp_stream
            .put_file(&url, &mut "".as_bytes())
            .map_err(|e| new_request_put_error("put", &url, e))?;
        Ok(n)
    }

    pub(crate) async fn ftp_mkdir(&self, path: &str) -> Result<()> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;
        ftp_stream
            .mkdir(&url)
            .map_err(|e| new_request_mkdir_error("mkdir", &url, e))?;

        Ok(())
    }

    pub(crate) async fn ftp_delete(&self, path: &str) -> Result<()> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        ftp_stream
            .rm(&url)
            .map_err(|e| new_request_remove_error("delete", &url, e))?;

        Ok(())
    }

    pub(crate) async fn ftp_append<R>(&self, path: &str, r: R) -> Result<u64>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let mut reader = Box::pin(r);

        let mut buf = Vec::new();

        reader.read_to_end(&mut buf);

        let n = ftp_stream
            .append_file(&url, &mut buf.as_slice())
            .map_err(|e| new_request_append_error("append", &url, e))?;

        Ok(n)
    }

    pub(crate) async fn ftp_stat(&self, path: &str) -> Result<File> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let mut buf = String::new();

        ftp_stream
            .retr(&url, |stream| {
                stream
                    .read_to_string(&mut buf)
                    .map_err(|e| FtpError::ConnectionError(e))
            })
            .map_err(|e| new_request_retr_error("stat", &url, e))?;

        ftp_stream
            .quit()
            .map_err(|e| new_request_quit_error("stat", &url, e))?;

        let file =
            File::from_str(buf.as_str()).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        Ok(file)
    }

    pub(crate) async fn ftp_list(&self, path: &str) -> Result<ReadDir> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let files = ftp_stream
            .list(Some(url.as_ref()))
            .map_err(|e| new_request_list_error("list", &url, e))?;

        let dir = ReadDir::new(files);

        Ok(dir)
    }
}

