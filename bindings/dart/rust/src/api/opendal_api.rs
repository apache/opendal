use flutter_rust_bridge::frb;

#[frb(opaque)]
pub struct Operator(opendal::Operator);
