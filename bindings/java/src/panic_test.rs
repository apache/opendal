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

use std::sync::Arc;

use jni::EnvUnowned;
use jni::objects::JClass;
use jni::sys::jlong;
use opendal::raw::{
    Layer, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
    RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer,
};
use opendal::{Capability, OperationContext, Operator};

use crate::error::ThrowException;

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_test_NativePanicTest_panicInNativeMethod<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) {
    env.with_env(|_| -> crate::Result<()> { panic!("panic in native method") })
        .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_test_NativePanicTest_panicOnStat<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *const Operator,
) -> jlong {
    env.with_env(|_| -> crate::Result<_> {
        let op = unsafe { &*op }.clone().layer(PanicLayer);
        Ok(Box::into_raw(Box::new(op)) as jlong)
    })
    .resolve::<ThrowException>()
}

#[derive(Debug)]
struct PanicLayer;

impl Layer for PanicLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(PanicService(inner.info()))
    }
}

#[derive(Debug)]
struct PanicService(ServiceInfo);

impl Service for PanicService {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.0.clone()
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            ..Default::default()
        }
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> opendal::Result<RpStat> {
        tokio::task::yield_now().await;
        panic!("panic during stat");
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        ().create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> opendal::Result<()> {
        ().read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> opendal::Result<()> {
        ().write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<()> {
        ().delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> opendal::Result<()> {
        ().list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> opendal::Result<()> {
        ().copy(ctx, from, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        ().rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        ().presign(ctx, path, args).await
    }
}
