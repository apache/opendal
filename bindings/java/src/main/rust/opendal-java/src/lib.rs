
use jni::JNIEnv;
use jni::objects::{JClass, JString};

use opendal::{BlockingOperator, Operator};
use opendal::services::Fs;

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_FileSystemOperator_getOperator(
    mut env: JNIEnv,
    _class: JClass,
    root: JString,
) -> *const i32 {
    let mut builder = Fs::default();
    let input: String = env
        .get_string(&root)
        .expect("Couldn't get java string!")
        .into();
    builder.root(&input);
    let op = Operator::new(builder).unwrap().finish().blocking();
    let ptr = Box::new(op);
    Box::into_raw(ptr) as *const i32
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_FileSystemOperator_freeOperator(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
) {
    unsafe { Box::from_raw(ptr) };
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_FileSystemOperator_write(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
    content: JString
) {

    let op = unsafe { &mut *ptr };
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let content: String = env
        .get_string(&content)
        .expect("Couldn't get java string!")
        .into();
    op.write(&file, content).unwrap();
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_FileSystemOperator_read<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) -> JString<'local> {

    let op = unsafe { &mut *ptr };
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let content = String::from_utf8(op.read(&file).unwrap())
        .expect("Couldn't convert to string");

    let output = env
        .new_string(format!("{}", content))
        .expect("Couldn't create java string!");
    output
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_FileSystemOperator_delete<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) {

    let op = unsafe { &mut *ptr };
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    op.delete(&file).unwrap();
}