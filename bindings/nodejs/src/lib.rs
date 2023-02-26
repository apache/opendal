#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use opendal::services;
use opendal::Operator;

#[napi]
pub fn debug() -> String {
    let op = Operator::create(services::Memory::default())
        .unwrap()
        .finish();
    format!("{:?}", op.metadata())
}
