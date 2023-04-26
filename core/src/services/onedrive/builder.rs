use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use log::debug;

use super::backend::OneDriveBackend;
use crate::raw::{normalize_root, HttpClient};
use crate::Scheme;
use crate::*;

#[derive(Default)]
pub struct OneDriveBuilder {
    access_token: Option<String>,
    root: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for OneDriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.access_token);
        de.finish()
    }
}

impl OneDriveBuilder {
    fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.access_token = Some(access_token.to_string());
        self
    }

    fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());
        self
    }

    fn http_client(&mut self, http_client: HttpClient) -> &mut Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for OneDriveBuilder {
    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Onedrive)
            })?
        };

        match self.access_token.clone() {
            Some(access_token) => Ok(OneDriveBackend::new(root, access_token, client)),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "access_token not set")),
        }
    }

    const SCHEME: Scheme = Scheme::Onedrive;

    type Accessor = OneDriveBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = Self::default();

        map.get("root").map(|v| builder.root(v));
        map.get("access_token").map(|v| builder.access_token(v));

        builder
    }
}
