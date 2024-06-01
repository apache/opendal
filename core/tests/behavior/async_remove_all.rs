use anyhow::Result;
use futures::TryStreamExt;
use libtest_mimic::Trial;
use opendal::{BlockingOperator, Operator};

use crate::gen_bytes;
use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.write && cap.list_with_recursive {
        tests.extend(async_trials!(op, test_blocking_remove_all_basic));
        if !cap.create_dir {
            tests.extend(async_trials!(
                op,
                test_blocking_remove_all_with_prefix_exists
            ));
        }
    }

    if !cap.create_dir {}
}

async fn test_blocking_remove_all_with_objects(
    op: Operator,
    paths: impl IntoIterator<Item = &'static str>,
) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    for path in paths {
        let path = format!("{parent}/{path}");
        let (content, _) = gen_bytes(op.info().full_capability());
        op.write(&path, content).await.expect("write must succeed");
    }

    op.remove_all(&parent).await?;

    let found = op
        .lister_with(&format!("{parent}/"))
        .recursive(true)
        .await
        .expect("list must succeed")
        .try_next()
        .await
        .expect("list must succeed")
        .is_some();

    assert!(!found, "all objects should be removed");

    Ok(())
}

pub async fn test_blocking_remove_all_basic(op: Operator) -> Result<()> {
    test_blocking_remove_all_with_objects(op, ["a/b", "a/c", "a/d/e"]).await
}

pub async fn test_blocking_remove_all_with_prefix_exists(op: Operator) -> Result<()> {
    test_blocking_remove_all_with_objects(op, ["a", "a/b", "a/c", "a/b/e"]).await
}
