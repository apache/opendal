// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::str::FromStr;

use ::opendal as od;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3_asyncio::tokio::future_into_py;

create_exception!(opendal, Error, PyException);

fn build_operator(scheme: od::Scheme, map: HashMap<String, String>) -> PyResult<od::Operator> {
    use od::services::*;

    let op = match scheme {
        od::Scheme::Azblob => od::Operator::from_map::<Azblob>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Azdfs => od::Operator::from_map::<Azdfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Fs => od::Operator::from_map::<Fs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Gcs => od::Operator::from_map::<Gcs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Ghac => od::Operator::from_map::<Ghac>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Http => od::Operator::from_map::<Http>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Ipmfs => od::Operator::from_map::<Ipmfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Memory => od::Operator::from_map::<Memory>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Obs => od::Operator::from_map::<Obs>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Oss => od::Operator::from_map::<Oss>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::S3 => od::Operator::from_map::<S3>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Webdav => od::Operator::from_map::<Webdav>(map)
            .map_err(format_pyerr)?
            .finish(),
        od::Scheme::Webhdfs => od::Operator::from_map::<Webhdfs>(map)
            .map_err(format_pyerr)?
            .finish(),
        _ => Err(PyNotImplementedError::new_err(format!(
            "unsupported scheme `{scheme}`"
        )))?,
    };

    Ok(op)
}

#[pyclass(module = "opendal")]
struct AsyncOperator(od::Operator);

#[pymethods]
impl AsyncOperator {
    #[new]
    #[pyo3(signature = (scheme, **map))]
    pub fn new(scheme: &str, map: Option<&PyDict>) -> PyResult<Self> {
        let scheme = od::Scheme::from_str(scheme)
            .map_err(|err| {
                od::Error::new(od::ErrorKind::Unexpected, "not supported scheme").set_source(err)
            })
            .map_err(format_pyerr)?;
        let map = map
            .map(|v| {
                v.extract::<HashMap<String, String>>()
                    .expect("must be valid hashmap")
            })
            .unwrap_or_default();

        Ok(AsyncOperator(build_operator(scheme, map)?))
    }

    pub fn read<'p>(&'p self, py: Python<'p>, path: &'p str) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        let path = path.to_string();
        future_into_py(py, async move {
            let res: Vec<u8> = this.read(&path).await.map_err(format_pyerr)?;
            let pybytes: PyObject = Python::with_gil(|py| PyBytes::new(py, &res).into());
            Ok(pybytes)
        })
    }

    pub fn write<'p>(&'p self, py: Python<'p>, path: &'p str, bs: Vec<u8>) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        let path = path.to_string();
        future_into_py(py, async move {
            this.write(&path, bs).await.map_err(format_pyerr)
        })
    }

    pub fn stat<'p>(&'p self, py: Python<'p>, path: &'p str) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        let path = path.to_string();
        future_into_py(py, async move {
            let res: Metadata = this.stat(&path).await.map_err(format_pyerr).map(Metadata)?;

            Ok(res)
        })
    }

    pub fn create_dir<'p>(&'p self, py: Python<'p>, path: &'p str) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        let path = path.to_string();
        future_into_py(py, async move {
            this.create_dir(&path).await.map_err(format_pyerr)
        })
    }

    pub fn delete<'p>(&'p self, py: Python<'p>, path: &'p str) -> PyResult<&'p PyAny> {
        let this = self.0.clone();
        let path = path.to_string();
        future_into_py(
            py,
            async move { this.delete(&path).await.map_err(format_pyerr) },
        )
    }
}

#[pyclass(module = "opendal")]
struct Operator(od::BlockingOperator);

#[pymethods]
impl Operator {
    #[new]
    #[pyo3(signature = (scheme="", **map))]
    pub fn new(scheme: &str, map: Option<&PyDict>) -> PyResult<Self> {
        let scheme = od::Scheme::from_str(scheme)
            .map_err(|err| {
                od::Error::new(od::ErrorKind::Unexpected, "not supported scheme").set_source(err)
            })
            .map_err(format_pyerr)?;
        let map = map
            .map(|v| {
                v.extract::<HashMap<String, String>>()
                    .expect("must be valid hashmap")
            })
            .unwrap_or_default();

        Ok(Operator(build_operator(scheme, map)?.blocking()))
    }

    pub fn read<'p>(&'p self, py: Python<'p>, path: &str) -> PyResult<&'p PyAny> {
        self.0
            .read(path)
            .map_err(format_pyerr)
            .map(|res| PyBytes::new(py, &res).into())
    }

    pub fn open_reader(&self, path: &str) -> PyResult<Reader> {
        self.0
            .reader(path)
            .map(|reader| Reader(Some(reader)))
            .map_err(format_pyerr)
    }

    pub fn write(&self, path: &str, bs: Vec<u8>) -> PyResult<()> {
        self.0.write(path, bs).map_err(format_pyerr)
    }

    pub fn stat(&self, path: &str) -> PyResult<Metadata> {
        self.0.stat(path).map_err(format_pyerr).map(Metadata)
    }

    pub fn create_dir(&self, path: &str) -> PyResult<()> {
        self.0.create_dir(path).map_err(format_pyerr)
    }

    pub fn delete(&self, path: &str) -> PyResult<()> {
        self.0.delete(path).map_err(format_pyerr)
    }

    pub fn list(&self, path: &str) -> PyResult<BlockingLister> {
        Ok(BlockingLister(self.0.list(path).map_err(format_pyerr)?))
    }

    pub fn scan(&self, path: &str) -> PyResult<BlockingLister> {
        Ok(BlockingLister(self.0.scan(path).map_err(format_pyerr)?))
    }
}

#[pyclass(module = "opendal")]
struct Reader(Option<od::BlockingReader>);

impl Reader {
    fn as_mut(&mut self) -> PyResult<&mut od::BlockingReader> {
        let reader = self
            .0
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("I/O operation on closed file."))?;
        Ok(reader)
    }
}

#[pymethods]
impl Reader {
    #[pyo3(signature = (size=None,))]
    pub fn read<'p>(&'p mut self, py: Python<'p>, size: Option<usize>) -> PyResult<&'p PyAny> {
        let reader = self.as_mut()?;
        let buffer = match size {
            Some(size) => {
                let mut buffer = vec![0; size];
                reader
                    .read_exact(&mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
            None => {
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                buffer
            }
        };
        Ok(PyBytes::new(py, &buffer).into())
    }

    pub fn write(&mut self, _bs: &[u8]) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "BlockingReader does not support write",
        ))
    }

    #[pyo3(signature = (pos, whence = 0))]
    pub fn seek(&mut self, pos: i64, whence: u8) -> PyResult<u64> {
        let whence = match whence {
            0 => SeekFrom::Start(pos as u64),
            1 => SeekFrom::Current(pos),
            2 => SeekFrom::End(pos),
            _ => return Err(PyValueError::new_err("invalid whence")),
        };
        let reader = self.as_mut()?;
        reader
            .seek(whence)
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    pub fn tell(&mut self) -> PyResult<u64> {
        let reader = self.as_mut()?;
        reader
            .stream_position()
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {
        drop(self.0.take());
    }
}

#[pyclass(unsendable, module = "opendal")]
struct BlockingLister(od::BlockingLister);

#[pymethods]
impl BlockingLister {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
        match slf.0.next() {
            Some(Ok(entry)) => Ok(Some(Entry(entry).into_py(slf.py()))),
            Some(Err(err)) => {
                let pyerr = format_pyerr(err);
                Err(pyerr)
            }
            None => Ok(None),
        }
    }
}

#[pyclass(module = "opendal")]
struct Entry(od::Entry);

#[pymethods]
impl Entry {
    #[getter]
    pub fn path(&self) -> &str {
        self.0.path()
    }

    fn __str__(&self) -> &str {
        self.0.path()
    }

    fn __repr__(&self) -> String {
        format!("Entry({:?})", self.0.path())
    }
}

#[pyclass(module = "opendal")]
struct Metadata(od::Metadata);

#[pymethods]
impl Metadata {
    #[getter]
    pub fn content_disposition(&self) -> Option<&str> {
        self.0.content_disposition()
    }

    #[getter]
    pub fn content_length(&self) -> u64 {
        self.0.content_length()
    }

    #[getter]
    pub fn content_md5(&self) -> Option<&str> {
        self.0.content_md5()
    }

    #[getter]
    pub fn content_type(&self) -> Option<&str> {
        self.0.content_type()
    }

    #[getter]
    pub fn etag(&self) -> Option<&str> {
        self.0.etag()
    }

    #[getter]
    pub fn mode(&self) -> EntryMode {
        EntryMode(self.0.mode())
    }
}

#[pyclass(module = "opendal")]
struct EntryMode(od::EntryMode);

#[pymethods]
impl EntryMode {
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    pub fn __repr__(&self) -> &'static str {
        match self.0 {
            od::EntryMode::FILE => "EntryMode.FILE",
            od::EntryMode::DIR => "EntryMode.DIR",
            od::EntryMode::Unknown => "EntryMode.UNKNOWN",
        }
    }
}

fn format_pyerr(err: od::Error) -> PyErr {
    use od::ErrorKind::*;
    match err.kind() {
        NotFound => PyFileNotFoundError::new_err(err.to_string()),
        _ => Error::new_err(err.to_string()),
    }
}

#[pymodule]
fn opendal(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Operator>()?;
    m.add_class::<Reader>()?;
    m.add_class::<AsyncOperator>()?;
    m.add_class::<Entry>()?;
    m.add_class::<EntryMode>()?;
    m.add_class::<Metadata>()?;
    m.add("Error", py.get_type::<Error>())?;
    Ok(())
}
