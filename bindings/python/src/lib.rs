use ::opendal::Operator;
use ::opendal::Scheme;
use pyo3::prelude::*;

#[pyfunction]
fn debug() -> PyResult<String> {
    let op = Operator::from_env(Scheme::Fs).unwrap();
    Ok(format!("{:?}", op.metadata()))
}

#[pymodule]
fn opendal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(debug, m)?)?;
    Ok(())
}
