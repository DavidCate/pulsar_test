import pulsar
from pulsar_test.admin import Admin
import asyncio

async def test():
    client = pulsar.Client('pulsar://47.94.142.82:6650')



    info = {
        'ip': '47.94.142.82',
        'port': 16650,
        'ispersistent': True,
    }
    consumer_type = pulsar.ConsumerType.Exclusive
    consumer = client.subscribe('topic-1', 'sub-1', consumer_type=consumer_type, schema=pulsar.schema.BytesSchema())
    print(consumer.topic())
    while True:
        msg = consumer.receive()

        print(msg)
        consumer.acknowledge(msg)


if __name__=='__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())