#[cfg(test)]
mod tests {

    use anyhow::Result;
    use opendal::*;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    use crate::*;

    #[wasm_bindgen_test]
    /// Create dir with dir path should succeed.
    pub async fn test_create_dir() -> Result<()> {
        let op = operator();
        let path = TEST_FIXTURE.new_dir_path();

        op.create_dir(&path).await?;

        let meta = op.stat(&path).await?;
        assert_eq!(meta.mode(), EntryMode::DIR);
        Ok(())
    }

    /// Create dir on existing dir should succeed.
    #[wasm_bindgen_test]
    pub async fn test_create_dir_existing() -> Result<()> {
        let op = operator();
        let path = TEST_FIXTURE.new_dir_path();

        op.create_dir(&path).await?;

        op.create_dir(&path).await?;

        let meta = op.stat(&path).await?;
        assert_eq!(meta.mode(), EntryMode::DIR);

        Ok(())
    }
}
