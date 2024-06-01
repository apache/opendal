use anyhow::Result;
use libtest_mimic::Trial;
use opendal::{BlockingOperator, Operator};

use crate::gen_bytes;
use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.write && cap.list_with_recursive {
        tests.extend(blocking_trials!(op, test_blocking_remove_all_basic));
        if !cap.create_dir {
            tests.extend(blocking_trials!(
                op,
                test_blocking_remove_all_with_prefix_exists
            ));
        }
    }

    if !cap.create_dir {}
}

fn test_blocking_remove_all_with_objects(
    op: BlockingOperator,
    paths: impl IntoIterator<Item = &'static str>,
) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    for path in paths {
        let path = format!("{parent}/{path}");
        let (content, _) = gen_bytes(op.info().full_capability());
        op.write(&path, content).expect("write must succeed");
    }

    op.remove_all(&parent)?;

    let found = op
        .lister_with(&format!("{parent}/"))
        .recursive(true)
        .call()
        .expect("list must succed")
        .into_iter()
        .next()
        .is_some();

    assert!(!found, "all objects should be removed");

    Ok(())
}

pub fn test_blocking_remove_all_basic(op: BlockingOperator) -> Result<()> {
    test_blocking_remove_all_with_objects(op, ["a/b", "a/c", "a/d/e"])
}

pub fn test_blocking_remove_all_with_prefix_exists(op: BlockingOperator) -> Result<()> {
    test_blocking_remove_all_with_objects(op, ["a", "a/b", "a/c", "a/b/e"])
}
