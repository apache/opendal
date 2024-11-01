use crate::{raw::oio, Buffer, Result};

pub struct OpfsReader {}

impl oio::Read for OpfsReader {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        panic!()
    }
}

impl oio::BlockingRead for OpfsReader {
    fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        panic!()
    }
}
