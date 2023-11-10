import asyncio



async def main():
    print('asd')
    consumer_task = asyncio.create_task(consumer())
    producer_task = asyncio.create_task(producer())

    await asyncio.wait([consumer_task, producer_task])

asyncio.run(main())