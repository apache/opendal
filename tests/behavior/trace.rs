use anyhow::Result;
use minitrace::prelude::*;
use std::future::Future;

pub async fn jaeger_tracing<F>(future: F, end_point: &str, service_name: &str) -> Result<()>
where
    F: Future<Output = Result<(), anyhow::Error>>,
{
    let collector = {
        let (root, collector) = Span::root("root");
        let _g = root.set_local_parent();

        future.await?;
        collector
    };
    let spans = collector.collect().await;
    // Report to Jaeger
    let bytes =
        minitrace_jaeger::encode(service_name.to_owned(), rand::random(), 0, 0, &spans).unwrap();
    minitrace_jaeger::report(end_point.parse().unwrap(), &bytes)
        .await
        .ok();
    Ok(())
}
