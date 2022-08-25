use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::io::Result;
//use std::io::Error;
use std::str;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncSeekExt;
use futures::AsyncReadExt;
use log::info;
use suppaftp::native_tls::{TlsConnector, TlsStream};
use suppaftp::types::{FileType, Response};
use suppaftp::{FtpError, FtpResult, FtpStream, Status};

use super::err::new_request_connection_error;
use super::err::new_request_quit_error;
use super::err::new_request_secure_error;
use super::err::new_request_sign_error;
use super::err::new_request_retr_error;
use super::err::new_request_put_error;
use super::err::parse_error;
use super::err::parse_io_error;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::error::other;
use crate::error::BackendError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesReader;

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

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        unimplemented!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let reader = self
            .ftp_get(&args.path())
            .await?;
        let mut reader = Compat::new(reader);

        if let Some(offset) = args.offset(){
            reader.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| parse_io_error(e, "read", args.path()))?;
        }

        let r: BytesReader = match args.size() {
            Some(size) => Box::new(reader.take(size)),
            None => Box::new(reader),
        };

        Ok(r)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64>{
        unimplemented!()
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata>{
        unimplemented!()
    }

    async fn delete(&self, args: &OpDelete) -> Result<()>{
        unimplemented!()
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer>{
        unimplemented!()
    }
}

impl Backend {
    pub(crate) fn ftp_connect(&self, url: &String) -> Result<FtpStream> {
        let mut ftp_stream = FtpStream::connect(url)
        .map_err(|e| new_request_connection_error("connection", &url, e))?;

        // switch to secure mode if tls mode is on.
        if self.tls {
            ftp_stream = ftp_stream
                .into_secure(TlsConnector::new(), &self.endpoint)
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
        
        let reader = ftp_stream.retr_as_buffer(&url)
        .map_err(|e| new_request_retr_error("get", &url, e))?;

        ftp_stream
            .quit()
            .map_err(|e| new_request_quit_error("get", &url, e))?;
        
           Ok(reader)
    }

    pub(crate) async fn ftp_put(&self, path: &str, fi) -> Result<TcpStream> {
        let url = format!("{}/{}:{}", self.endpoint, path, self.port);

        let mut ftp_stream = self.ftp_connect(&url)?;

        let reader = ftp_stream.put_with_stream(&url)
        .map_err(|e| new_request_put_error("put", &url, e))?
        .into_tcp_stream();
        Ok(reader)
    }
    
    pub(crate) async fn ftp_delete(&self) -> Result<()>{

    }


}
