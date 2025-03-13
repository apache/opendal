use crate::{raw::oio, Buffer, Metadata, Result};

pub struct OpfsWriter {}

impl oio::Write for OpfsWriter {
    async fn abort(&mut self) -> Result<()> {
        panic!()
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(Metadata::default())
    }

    async fn write(&mut self, bs: Buffer) -> Result<()> {
        panic!()
    }
}

impl oio::BlockingWrite for OpfsWriter {
    fn close(&mut self) -> Result<Metadata> {
        Ok(Metadata::default())
    }

    fn write(&mut self, _bs: Buffer) -> Result<()> {
        panic!()
    }
}
