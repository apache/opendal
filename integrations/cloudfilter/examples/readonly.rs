use std::{
    env,
    path::{Path, PathBuf},
};

use cloud_filter::root::{
    HydrationType, PopulationType, Registration, SecurityId, Session, SyncRootIdBuilder,
};
use opendal::{services, Operator};
use tokio::signal;
use widestring::{u16str, U16String};

const PROVIDER_NAME: &str = "ro-cloudfilter";
const DISPLAY_NAME: &str = "Read Only Cloud Filter";

#[tokio::main]
async fn main() {
    env_logger::init();

    let root = env::var("ROOT").expect("$ROOT is set");
    let client_path = env::var("CLIENT_PATH").expect("$CLIENT_PATH is set");

    let mut fs = services::Fs::default().root(&root);

    let op = Operator::new(fs).expect("build operator").finish();

    let sync_root_id = SyncRootIdBuilder::new(U16String::from_str(PROVIDER_NAME))
        .user_security_id(SecurityId::current_user().unwrap())
        .build();

    if !sync_root_id.is_registered().unwrap() {
        let u16_display_name = U16String::from_str(DISPLAY_NAME);
        Registration::from_sync_root_id(&sync_root_id)
            .display_name(&u16_display_name)
            .hydration_type(HydrationType::Full)
            .population_type(PopulationType::Full)
            .icon(
                U16String::from_str("%SystemRoot%\\system32\\charmap.exe"),
                0,
            )
            .version(u16str!("1.0.0"))
            .recycle_bin_uri(u16str!("http://cloudmirror.example.com/recyclebin"))
            .register(Path::new(&client_path))
            .unwrap();
    }

    let connection = Session::new()
        .connect(
            &client_path,
            cloudfilter_opendal::CloudFilter::new(op, PathBuf::from(&client_path)),
        )
        .expect("create session");

    signal::ctrl_c().await.unwrap();

    drop(connection);
    sync_root_id.unregister().unwrap();
}
