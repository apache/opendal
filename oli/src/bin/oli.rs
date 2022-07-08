// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The main oli command-line interface
//!
//! The oli binary is a chimera, changing its behavior based on the
//! name of the binary. It works like `rustup`: when the binary is called
//! 'oli' or 'oli.exe' it offers the oli command-line interface, and
//! when it is called 'ocp' it behaves as a proxy to 'oli cp'.

fn main() {
    println!("Hello, World!")
}
