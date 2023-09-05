import asyncio
import random

import aio_pika


async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
):
    async with message.process():
        print(message.body)
        await asyncio.sleep(2)


async def consumer():
    print('consumer')
    connection = await aio_pika.connect_robust(
        "amqp://user:password@127.0.0.1/",
    )

    queue_name = 'message_queue'


    #async with connection:
    channel = await connection.channel()

    # Will take no more than 10 messages in advance
    await channel.set_qos(prefetch_count=30)

    queue = await channel.declare_queue(queue_name, auto_delete=True)

    while True:
        await queue.consume(process_message)

    await connection.close()

async def producer():
    print('producer')
    connection = await aio_pika.connect_robust(
        "amqp://user:password@127.0.0.1/",
    )

    async with connection:
        queue_name = 'message_queue'
        channel = await connection.channel()

        count = 0
        while True:
            await channel.default_exchange.publish(
                aio_pika.Message(body=f"message {count}".encode()),
                routing_key=queue_name,
            )
            count += 1
            if count % 10 == 0:
                await asyncio.sleep(random.randint(0, 3))
            # if count % 100 == 0:
            #     await asyncio.sleep(2)


async def main():
    print('asd')
    consumer_task = asyncio.create_task(consumer())
    producer_task = asyncio.create_task(producer())

    await asyncio.wait([consumer_task, producer_task])


if __name__ == '__main__':
    asyncio.run(main())

