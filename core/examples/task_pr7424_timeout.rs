use std::env;
use std::time::Instant;

use opendal::ErrorKind;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let expected = env::var("TASK_PR7424_EXPECTED")?;
    let endpoint = env::var("TASK_PR7424_ENDPOINT")?;
    let key = env::var("TASK_PR7424_KEY")?;
    let op = Operator::via_iter(
        "sftp",
        [
            ("endpoint".to_string(), endpoint),
            ("user".to_string(), "foo".to_string()),
            ("key".to_string(), key),
            ("known_hosts_strategy".to_string(), "accept".to_string()),
            ("root".to_string(), "/".to_string()),
        ],
    )?;
    let start = Instant::now();
    let err = op
        .stat("timeout-boundary-probe")
        .await
        .expect_err("the real sshd intentionally rejects authentication");
    let elapsed = start.elapsed().as_millis();
    let has_source = std::error::Error::source(&err).is_some();
    let detail = format!("{err:?}");
    let generic = detail.contains("connection request: timeout")
        || detail.contains("waiting for SFTP connection capacity timed out");
    println!(
        "OBSERVATION expected={expected} elapsed_ms={elapsed} kind={:?} temporary={} source={has_source} generic_timeout={generic}",
        err.kind(),
        err.is_temporary()
    );
    println!("ERROR_CHAIN {detail}");
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(!generic && has_source && !err.is_temporary());
    assert!(detail.contains("Permission denied (publickey)"));
    match expected.as_str() {
        "slow" => assert!(elapsed >= 11000, "the delayed authentication must execute"),
        "fast" => {}
        _ => panic!("unknown expectation"),
    }
    Ok(())
}
