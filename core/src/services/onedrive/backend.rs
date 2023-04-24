use async_trait::async_trait;
use std::fmt::Debug;

use crate::raw::{Accessor, AccessorInfo, IncomingAsyncBody};

#[derive(Clone)]
pub struct OneDriveBackend {
    root: String,
    access_token: String,
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
}
