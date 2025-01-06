use std::env;

use cloud_filter::root::{
    HydrationType, PopulationType, SecurityId, Session, SyncRootIdBuilder, SyncRootInfo,
};
use opendal::{services, Operator};
use tokio::{runtime::Handle, signal};

const PROVIDER_NAME: &str = "ro-cloud_filter";
const DISPLAY_NAME: &str = "Read Only Cloud Filter";

#[tokio::main]
async fn main() {
    env_logger::init();

    let root = env::var("ROOT").expect("$ROOT is set");
    let client_path = env::var("CLIENT_PATH").expect("$CLIENT_PATH is set");

    let fs = services::Fs::default().root(&root);

    let op = Operator::new(fs).expect("build operator").finish();

    let sync_root_id = SyncRootIdBuilder::new(PROVIDER_NAME)
        .user_security_id(SecurityId::current_user().unwrap())
        .build();

    if !sync_root_id.is_registered().unwrap() {
        sync_root_id
            .register(
                SyncRootInfo::default()
                    .with_display_name(DISPLAY_NAME)
                    .with_hydration_type(HydrationType::Full)
                    .with_population_type(PopulationType::Full)
                    .with_icon("%SystemRoot%\\system32\\charmap.exe,0")
                    .with_version("1.0.0")
                    .with_recycle_bin_uri("http://cloudmirror.example.com/recyclebin")
                    .unwrap()
                    .with_path(&client_path)
                    .unwrap(),
            )
            .unwrap();
    }

    let handle = Handle::current();
    let connection = Session::new()
        .connect_async(
            &client_path,
            cloud_filter_opendal::CloudFilter::new(op, client_path.clone().into()),
            move |f| handle.block_on(f),
        )
        .expect("create session");

    signal::ctrl_c().await.unwrap();

    drop(connection);
    sync_root_id.unregister().unwrap();
}
