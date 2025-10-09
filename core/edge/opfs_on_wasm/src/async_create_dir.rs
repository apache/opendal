// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
