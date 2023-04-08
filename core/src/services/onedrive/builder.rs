use std::fmt::{Debug, Formatter};

#[derive(Default)]
pub struct OneDriveBuilder {
    access_token: Option<String>,
}

impl Debug for OneDriveBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("Builder");
        de.field("endpoint", &self.access_token);
        de.finish()
    }
}

impl OneDriveBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn access_token(mut self, access_token: &str) -> Self {
        self.access_token = Some(access_token.to_string());
        self
    }

    // pub fn build(self) -> OneDrive {
    //     OneDrive {
    //         access_token: self.access_token,
    //     }
    // }
}
