import asyncio


async def inner_task1():
    print('inner1')

async def inner_task2():
    print('inne2')

async def task1():
    print('task1')
    await asyncio.wait([
        asyncio.create_task(inner_task1()),
        asyncio.create_task(inner_task2()),
    ])
    print('inners finished')


async def task2():
    await asyncio.sleep(2)
    print('task2')

async def main():
    await asyncio.wait([
        asyncio.create_task(task1()),
        asyncio.create_task(task2())
    ])

asyncio.run(main())