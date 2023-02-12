use crate::Result;
use crate::{
    raw::{normalize_path, output},
    ObjectMetadata, ObjectMode,
};
use async_trait::async_trait;

use super::list_response::Multistatus;
use std::path::{Path, PathBuf};

pub struct DirPager {
    root: PathBuf,

    size: usize,
    multistates: Multistatus,
}

impl DirPager {
    pub fn new(root: &Path, multistates: Multistatus, limit: Option<usize>) -> Self {
        Self {
            root: root.to_owned(),
            size: limit.unwrap_or(1000),
            multistates: multistates,
        }
    }
}

#[async_trait]
impl output::Page for DirPager {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let mut oes: Vec<output::Entry> = Vec::with_capacity(self.size);

        for i in 0..self.size {
            if i >= self.multistates.response.len() {
                break;
            }
            match self.multistates.response.get(0) {
                Some(de) => {
                    let path = PathBuf::from(de.href.clone());

                    let rel_path = normalize_path(
                        &path
                            .strip_prefix(&self.root)
                            .expect("cannot fail because the prefix is iterated")
                            .to_string_lossy()
                            .replace('\\', "/"),
                    );

                    let entry = if de.propstat.prop.resourcetype.value
                        == Some(super::list_response::ResourceType::Collection)
                    {
                        output::Entry::new(&rel_path, ObjectMetadata::new(ObjectMode::DIR))
                    } else {
                        output::Entry::new(&rel_path, ObjectMetadata::new(ObjectMode::FILE))
                    };

                    oes.push(entry);
                }
                None => break,
            }
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
