import asyncio
import random
import traceback
from datetime import datetime

import aio_pika
from asyncio import Queue

#global queue
global_queue = Queue()

async def send_message(message):
    a = 2.0 + 3.33
    return 1

async def process_queue():
    global global_queue

    send_count = 0
    while True:
        try:
            #if not global_queue.empty():
            message = await global_queue.get()
            print(message)
            send_count += await send_message(message)
            await asyncio.sleep(0.03)
        except Exception:
            print(traceback.format_exc())
            await asyncio.sleep(5)

async def add_to_queue(
    message: aio_pika.abc.AbstractIncomingMessage,
):
    global global_queue
    #await asyncio.sleep(1)
    async with message.process():
        #await global_queue.put(str(message.body))
        print(str(message.body))
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
    await channel.set_qos(prefetch_count=10)

    queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)

    await queue.consume(add_to_queue)
    # while True:
    #     #await asyncio.sleep(1)
    #     print('before', datetime.utcnow())
    #     await queue.consume(add_to_queue)
    #     print('after', datetime.utcnow())

    # try:
    #     # Wait until terminate
    #     await asyncio.Future()
    # finally:
    #     await connection.close()
    #await connection.close()

async def producer():
    print('producer')
    connection = await aio_pika.connect_robust(
        "amqp://user:password@127.0.0.1/",
    )
    async with connection:
        queue_name = 'message_queue'
        channel = await connection.channel()

        count = 0
        while count < 60:
            #print('before', datetime.utcnow())
            await channel.default_exchange.publish(
                aio_pika.Message(body=f"message {count}".encode()),
                routing_key=queue_name,
            )
            #print('after', datetime.utcnow())
            count += 1
            # if count % 10 == 0:
            #     await asyncio.sleep(random.randint(0, 3))
            # if count % 100 == 0:
            #     await asyncio.sleep(2)

async def delay(coro, seconds):
    await asyncio.sleep(seconds)
    await coro

async def main():
    print('asd')
    producer_task = asyncio.create_task(producer())
    consumer_task = asyncio.create_task(delay(consumer(), 2))
    process_messages = asyncio.create_task(process_queue())

    await asyncio.wait([consumer_task, producer_task, process_messages])


if __name__ == '__main__':
    asyncio.run(main())

