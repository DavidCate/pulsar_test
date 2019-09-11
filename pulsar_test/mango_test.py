import mango_pulsar_client
import pulsar



def test():
    SERVICE_URL = "pulsar://47.94.142.82:6650"
    REST_URL="http://47.94.142.82:16650"
    TOPIC = "persistent://public/default/liuchaozhi"
    SUBSCRIPTION = "sub-1"
    PUBLISH = "pub-1"

    mango_client = mango_pulsar_client.MangoPulsarClient(SERVICE_URL, REST_URL)



    mango_client.admin.persistent_topic.list_topics_under_namespace()

    print(mango_client.list_topics_under_namespace('public','default'))
    producer=mango_client.create_producer(
        TOPIC,
        PUBLISH
    )


    for index in range(10):
        producer.send("sdddfasdfdsfds".encode('utf-8'))

    consumer_type = pulsar.ConsumerType.Exclusive
    consumer = mango_client.subscribe(TOPIC, SUBSCRIPTION, consumer_type=consumer_type, schema=pulsar.schema.BytesSchema())
    print(consumer.topic())
    while True:
        msg = consumer.receive()
        print(msg)
        print(msg.value())
        consumer.acknowledge(msg)

    mango_client.close()

if __name__ == '__main__':
    test()