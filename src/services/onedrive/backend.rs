use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use http::{Method, StatusCode};

use reqwest::{header::{self, HeaderValue}, Client, Request, Response, Url};
use url::Url;

use super::{Accessor, AccessorCapability, AccessorHint, AccessorMetadata, ObjectMode};
use crate::{error::{new_request_build_error, parse_error, Error, Result}, input::{self, AsyncBody, IncomingAsyncBody, Reader}, rp::{
    BytesRange, ObjectMetadata, OpCreate, OpDelete, OpRead, OpStat, OpWrite, RpCreate,
    RpDelete, RpRead, RpStat, RpWrite,
}, Scheme};
use crate::ops::OpRead;
use crate::raw::{Accessor, AccessorCapability, AccessorHint, AccessorMetadata, AsyncBody, IncomingAsyncBody, RpRead};

/// OneDrive backend implementation.
pub struct OneDriveBackend {
    client: Arc<Client>,
    endpoint: String,
    token: String,
}


impl Debug for OneDriveBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl Accessor for OneDriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Pager = ();
    type BlockingPager = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::OneDrive)
            .set_capabilities(AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List)
            .set_hints(AccessorHint::ReadStreamable);
        ma
    }

    // read
    async fn read(&self, path: &str, range: Option<BytesRange>) -> Result<OpRead<Self::Reader>> {
        let resp = self.graph_api_request("GET", path, AsyncBody::empty()).await?;
        let reader = resp.into_body();
        let metadata = ObjectMetadata::default();
        Ok(OpRead::new(reader, metadata))
    }
}

impl OneDriveBackend {
    pub fn new(client: Arc<Client>, endpoint: String, token: String) -> Self {
        OneDriveBackend {
            client,
            endpoint,
            token,
        }
    }

    async fn graph_api_request(
        &self,
        method: &str,
        path: &str,
        body: AsyncBody,
    ) -> Result<Response> {
        let url = Url::parse(&format!("{}/me/drive/root:/{}", self.endpoint, path))?;
        let mut req = Request::new(method.parse()?, url);
        let token = HeaderValue::from_str(&format!("Bearer {}", self.token))?;
        req.headers_mut().insert(header::AUTHORIZATION, token);
        req.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        req.headers_mut().insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        req.headers_mut().insert(header::ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        let req = req.body(body).map_err(new_request_build_error)?;

        let resp = self.client.execute(req).await?;

        if !resp.status().is_success() {
            Err(Error::OneDriveError(resp.status().as_u16()))
        } else {
            Ok(resp)
        }
    }
}
