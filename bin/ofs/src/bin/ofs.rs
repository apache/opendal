use clap::Parser;
use config::App;
use fuser::Session;
use opendal::Operator;
use tap::{Pipe, Tap};
use tokio::{
    runtime::{self, Runtime},
    signal::unix::{signal, SignalKind},
    task::spawn_blocking,
};

use std::{process::ExitCode, sync::OnceLock};

use ofs::{config, dalfs, inode};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[inline]
fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        runtime::Builder::new_multi_thread()
            .enable_io()
            .build()
            .expect("failed to build tokio runtime")
    })
}

fn main() -> ExitCode {
    let config = config::App::parse();
    env_logger::init();

    if let Err(e) = runtime().block_on(run(config)) {
        log::error!("{e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

async fn run(config: App) -> Result<(), Box<dyn std::error::Error>> {
    let options = config.options.unwrap_or_default();

    let fs = dalfs::DalFs {
        op: Operator::via_map(config.r#type, options)?.tap(|op| log::debug!("operator: {op:?}")),
        inodes: inode::InodeStore::new(0o550, 1000, 1000), // Temporarilly hardcode
    };

    let mut session = Session::new(fs, config.mount_point.as_ref(), &[])?;
    let mut umounter = session.unmount_callable();
    let session_task = spawn_blocking(move || session.run());

    let mut hangup = signal(SignalKind::hangup())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;

    let mut received_signal = |signal: &str| {
        log::warn!("Received signal: {signal}");
        umounter.unmount()
    };

    tokio::select! {
        res = session_task => { res.expect("failed to join session") },
        _ = hangup.recv() => received_signal("SIGHUP"),
        _ = interrupt.recv() => received_signal("SIGINT"),
        _ = terminate.recv() => received_signal("SIGTERM"),
    }?
    .pipe(Ok)
}
