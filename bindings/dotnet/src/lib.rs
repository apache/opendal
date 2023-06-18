use std::collections::HashMap;
use std::os::raw::c_char;
use std::str::FromStr;

#[no_mangle]
pub extern "C" fn blocking_operator_construct(
    scheme: *const c_char,
) -> *const opendal::BlockingOperator {
    if scheme.is_null() {
        return std::ptr::null();
    }

    let scheme = match opendal::Scheme::from_str(unsafe {
        std::ffi::CStr::from_ptr(scheme).to_str().unwrap()
    }) {
        Ok(scheme) => scheme,
        Err(_) => return std::ptr::null(),
    };

    let mut map = HashMap::default();
    map.insert("root".to_string(), "/tmp".to_string());
    let op = match opendal::Operator::via_map(scheme, map) {
        Ok(op) => op.blocking(),
        Err(err) => {
            println!("err={err:?}");
            return std::ptr::null();
        }
    };

    Box::leak(Box::new(op))
}

#[no_mangle]
pub extern "C" fn blocking_operator_write(
    op: *const opendal::BlockingOperator,
    path: *const c_char,
    _content: *const c_char,
) {
    let op = unsafe { &*(op) };
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    println!("path={path:?}");
    // let content = unsafe { std::ffi::CStr::from_ptr(content).to_str().unwrap() };
    // println!("content={content:?}");
    op.write(path, "12345").unwrap();
    let res = op.read(path).unwrap();
    println!("res={res:?}");
}

#[no_mangle]
pub extern "C" fn blocking_operator_read(
    op: *const opendal::BlockingOperator,
    path: *const c_char,
) -> *const c_char {
    let op = unsafe { &*(op) };
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    println!("path={path:?}");
    let mut res = op.read(path).unwrap();
    res.push(0);
    println!("res={res:?}");
    std::ffi::CString::from_vec_with_nul(res)
        .unwrap()
        .into_raw()
}
