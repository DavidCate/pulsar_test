import mango_pulsar_client
import pulsar



def test():
    SERVICE_URL = "pulsar://47.94.142.82:6650"
    REST_URL="http://47.94.142.82:16650"
    TOPIC = "persistent://public/default/liuchaozhi"
    SUBSCRIPTION = "sub-1"
    PUBLISH = "pub-1"

    mango_client = mango_pulsar_client.MangoPulsarClient(SERVICE_URL, REST_URL)

    propducer=mango_client.create_producer('new-topic','consumer01')


    last_message_id=propducer.last_sequence_id()

    print(last_message_id)
    topic=propducer.topic()
    propducer.send('第一条消息'.encode('utf-8'))
    print(topic)
    print(last_message_id)
    print(propducer.last_sequence_id())

    #当topic中没有消息的时候消息的id是－１
    #第一条消息的id是0
    print(mango_client.admin.persistent_topic.list_topics_under_namespace('public','default'))
    print(mango_client.admin.persistent_topic.get_last_commit_message_id_of_topic('public','default','new-topic'))



    subscribe=mango_client.subscribe()














    #
    # mango_client.admin.persistent_topic.peek_nth_message_on_a_topic_subscription()
    #
    #
    #
    # res=mango_client.admin.persistent_topic.create_non_partitioned_topic('liuchaozhi','main','xiaodi')
    # print(res)
    #
    #
    # res=mango_client.admin.persistent_topic.delete('public','default','liuchaozhi')
    # print(res)
    #
    # res=mango_client.admin.persistent_topic.list_topics_under_namespace('public','default')
    # print(res)
    #
    # print(mango_client.admin.persistent_topic.list_topics_under_namespace('public','default'))
    # producer=mango_client.create_producer(
    #     TOPIC,
    #     PUBLISH
    # )
    #
    #
    # for index in range(10):
    #     producer.send("sdddfasdfdsfds".encode('utf-8'))
    #
    # consumer_type = pulsar.ConsumerType.Exclusive
    # consumer = mango_client.subscribe(TOPIC, SUBSCRIPTION, consumer_type=consumer_type, schema=pulsar.schema.BytesSchema())
    # print(consumer.topic())
    # while True:
    #     msg = consumer.receive()
    #     print(msg)
    #     print(msg.value())
    #     consumer.acknowledge(msg)
    #
    # mango_client.close()

if __name__ == '__main__':
    test()