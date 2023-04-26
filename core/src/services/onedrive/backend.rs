use async_trait::async_trait;
use http::{header, Request, Response, StatusCode};
use std::fmt::Debug;

use crate::{
    ops::OpRead,
    raw::{
        build_rooted_abs_path, new_request_build_error, parse_into_metadata, percent_encode_path,
        Accessor, AccessorInfo, AsyncBody, HttpClient, IncomingAsyncBody, RpRead,
    },
    types::Result,
    Capability,
};

use super::error::parse_error;

#[derive(Clone)]
pub struct OneDriveBackend {
    root: String,
    access_token: String,
    client: HttpClient,
}

impl OneDriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        Self {
            root,
            access_token,
            client: http_client,
        }
    }
}

impl Debug for OneDriveBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("OneDriveBackend");
        de.field("root", &self.root);
        de.field("access_token", &self.access_token);
        de.finish()
    }
}

#[async_trait]
impl Accessor for OneDriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(crate::Scheme::Onedrive)
            .set_root(&self.root)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                copy: true,
                rename: true,
                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.onedrive_get(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}

impl OneDriveBackend {
    const ONEDRIVE_ENDPOINT_PREFIX: &'static str =
        "https://graph.microsoft.com/v1.0/me/drive/root:";
    const ONEDRIVE_ENDPOINT_SUFFIX: &'static str = ":/content";

    async fn onedrive_get(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let path = build_rooted_abs_path(&self.root, path);

        let url: String = format!(
            "{}{}{}",
            OneDriveBackend::ONEDRIVE_ENDPOINT_PREFIX,
            percent_encode_path(&path),
            OneDriveBackend::ONEDRIVE_ENDPOINT_SUFFIX
        );

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.access_token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}
