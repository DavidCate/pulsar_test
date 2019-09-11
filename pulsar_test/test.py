import pulsar
from pulsar_test.admin import Admin
from pulsar import AuthenticationToken
import asyncio

async def test():





    # client = pulsar.Client('pulsar://pulsar.aimango.net:6650',authentication=AuthenticationToken('FLFyW0oLJ2Fi22KKCm21J18mblAT5ucEKU='))
    #
    # consumer_type = pulsar.ConsumerType.Exclusive
    # consumer = client.subscribe('topic-1', 'sub-1', consumer_type=consumer_type, schema=pulsar.schema.BytesSchema())
    # print(consumer.topic())
    # while True:
    #     msg = consumer.receive()
    #
    #     print(msg)
    #     consumer.acknowledge(msg)
    SERVICE_URL = "pulsar://47.94.142.82:6650"
    TOPIC = "persistent://public/default/liuchaozhi"
    SUBSCRIPTION = "sub-1"
    PUBLISH="pub-1"


    client = pulsar.Client(SERVICE_URL)

    producer=client.create_producer(
        TOPIC,
        PUBLISH
    )
    consumer = client.subscribe(
        TOPIC,
        SUBSCRIPTION,
        # 如果要限制接收器队列大小
        receiver_queue_size=10,
        consumer_type=pulsar.ConsumerType.Shared)
    for index in range(10):
        producer.send("sdddfasdfdsfds".encode('utf-8'))



if __name__=='__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())