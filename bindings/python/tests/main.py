import asyncio;
import opendal;

async def main():
    op = opendal.Operator();
    o = op.object("path/to/file");
    o.blocking_write(b"Hello, World!");
    x = await o.read();
    print(bytes(x).decode('utf-8'));

asyncio.run(main())
