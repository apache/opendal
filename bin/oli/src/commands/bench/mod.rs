use crate::config::Config;
use crate::params::config::ConfigParams;
use anyhow::Result;
use std::path::PathBuf;

mod report;
mod suite;

#[derive(Debug, clap::Parser)]
#[command(
    name = "bench",
    about = "Run benchmark against the storage backend",
    disable_version_flag = true
)]
pub struct BenchCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// Name of the profile to use.
    #[arg()]
    pub profile: String,
    /// Path to the benchmark config.
    #[arg(
        value_parser = clap::value_parser!(PathBuf),
    )]
    pub bench: PathBuf,
}

impl BenchCmd {
    pub async fn run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;
        let op = cfg.operator(&self.profile)?;
        let suite = suite::BenchSuite::load(&self.bench)?;

        tokio::task::spawn_blocking(move || {
            suite.run(op).expect("failed to run bench suite");
        })
        .await?;
        Ok(())
    }
}
