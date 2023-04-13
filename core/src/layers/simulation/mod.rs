#[cfg(madsim)]
mod mock;
#[cfg(not(madsim))]
mod real;

#[cfg(madsim)]
pub use mock::MadsimLayer;
#[cfg(not(madsim))]
pub use real::MadsimLayer;

#[cfg(test)]
mod test {
    use super::*;
    use crate::{services, EntryMode, Operator};

    #[madsim::test]
    async fn test_madsim_layer() {
        let mut builder = services::Fs::default();
        builder.root(".");

        // Init an operator
        let op = Operator::new(builder)
            .unwrap()
            // Init with logging layer enabled.
            .layer(MadsimLayer::default())
            .finish();

        let path = "hello.txt";
        let data = "Hello, World!";

        // Write data
        op.write("hello.txt", "Hello, World!").await.unwrap();
    }
        // // Read data
        // let bs = op.read(path).await.unwrap();
        // assert_eq!(bs, data.as_bytes());
        //
        // // Fetch metadata
        // let meta = op.stat("hello.txt").await.unwrap();
        // let mode = meta.mode();
        // let length = meta.content_length();
        // assert_eq!(mode, EntryMode::FILE);
        // assert_eq!(length, data.len() as u64);
        // // Delete
        // op.delete("hello.txt").await.unwrap();
}
