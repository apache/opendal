// use super::Backend;
// pub struct S3ObjectStream {
//     backend: Backend,
//     path: String,

//     token: String,
//     done: bool,
//     state: State,
// }

// enum State {
//     Idle,
//     Sending(BoxFuture<'static, Result<bytes::Bytes>>),
//     Listing((ListOutput, usize, usize)),
// }
