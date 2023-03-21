use magnus::{define_global_function, function, Error};
use opendal::{services::Memory, Operator};

fn hello_opendal() {
    let op = Operator::new(Memory::default()).unwrap().finish();
    println!("{op:?}")
}

#[magnus::init]
fn init() -> Result<(), Error> {
    define_global_function("hello_opendal", function!(hello_opendal, 0));
    Ok(())
}
