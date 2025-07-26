// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::patches::buffer_by_ordered::BufferByOrdered;

use futures::Stream;
use std::future::Future;

pub trait StreamExt: Stream {
    fn buffer_by_ordered<F>(self, max_size: usize) -> BufferByOrdered<Self, F>
    where
        Self: Sized,
        Self: Stream<Item = (F, usize)>,
        F: Future,
    {
        BufferByOrdered::new(self, max_size)
    }
}

impl<T: ?Sized> StreamExt for T where T: Stream {}
