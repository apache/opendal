use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    // `stub_info` is a function defined by `define_stub_info_gatherer!` macro.
    let stub = _opendal::stub_info()?;
    stub.generate()?;
    Ok(())
}
