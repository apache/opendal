import os
from random import randint
from uuid import uuid4

import pytest


@pytest.mark.need_capability("write", "delete", "stat")
def test_sync_exists(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    operator.write(filename, content)
    assert operator.exists(filename)
    assert not operator.exists(filename + "not_exists")


@pytest.mark.asyncio
@pytest.mark.need_capability("write", "delete", "stat")
async def test_async_exists(service_name, operator, async_operator):
    size = randint(1, 1024)
    filename = f"test_file_{str(uuid4())}.txt"
    content = os.urandom(size)
    size = len(content)
    await async_operator.write(filename, content)
    assert await async_operator.exists(filename)
    assert not await async_operator.exists(filename + "not_exists")
    await async_operator.delete(filename)
