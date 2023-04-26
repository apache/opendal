use std::fmt::{Debug, Formatter};

use log::debug;

use super::backend::OneDriveBackend;
use crate::raw::normalize_root;
use crate::Scheme;
use crate::*;

#[derive(Default)]
pub struct OneDriveBuilder {
    access_token: Option<String>,
    root: Option<String>,
}

impl Debug for OneDriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.access_token);
        de.finish()
    }
}

impl OneDriveBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn access_token(mut self, access_token: &str) -> Self {
        self.access_token = Some(access_token.to_string());
        self
    }
}

impl Builder for OneDriveBuilder {
    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        match self.access_token.clone() {
            Some(access_token) => Ok(OneDriveBackend::new(root, access_token)),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "access_token not set")),
        }
    }

    const SCHEME: Scheme = Scheme::Onedrive;

    type Accessor = OneDriveBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        todo!()
    }
}
