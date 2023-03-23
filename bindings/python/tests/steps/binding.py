# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from behave import given, when, then
from behave.api.async_step import async_run_until_complete
import opendal

@given('A new OpenDAL Blocking Operator')
def step_impl(context):
    context.op = opendal.Operator("memory")

@when('Blocking write path "{filename}" with content "{content}"')
def step_impl(context, filename, content):
    context.op.write(filename, content.encode())

@then('The blocking file "{filename}" should exist')
def step_impl(context, filename):
    context.op.stat(filename)

@then('The blocking file "{filename}" entry mode must be file')
def step_impl(context, filename):
    assert context.op.stat(filename).mode.is_file()

@then('The blocking file "{filename}" content length must be {size:d}')
def step_impl(context, filename, size):
    assert context.op.stat(filename).content_length == size

@then('The blocking file "{filename}" must have content "{content}"')
def step_impl(context, filename, content):
    bs = context.op.read(filename)
    assert bs == content.encode()

@given('A new OpenDAL Async Operator')
@async_run_until_complete
async def step_impl(context):
    context.op = opendal.AsyncOperator("memory")

@when('Async write path "{filename}" with content "{content}"')
@async_run_until_complete
async def step_impl(context, filename, content):
    await context.op.write(filename, content.encode())

@then('The async file "{filename}" should exist')
@async_run_until_complete
async def step_impl(context, filename):
    await context.op.stat(filename)

@then('The async file "{filename}" entry mode must be file')
@async_run_until_complete
async def step_impl(context, filename):
    meta = await context.op.stat(filename)
    assert meta.mode.is_file()

@then('The async file "{filename}" content length must be {size:d}')
@async_run_until_complete
async def step_impl(context, filename, size):
    meta = await context.op.stat(filename)
    assert meta.content_length == size

@then('The async file "{filename}" must have content "{content}"')
@async_run_until_complete
async def step_impl(context, filename, content):
    bs = await context.op.read(filename)
    assert bs == content.encode()
