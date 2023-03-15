use opendal::{services::Memory, Operator};

/// Hello, OpenDAL!
#[no_mangle]
pub extern "C" fn hello_opendal() {
    let op = Operator::new(Memory::default()).unwrap().finish();
    println!("{op:?}")
}
