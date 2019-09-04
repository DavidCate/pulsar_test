import pulsar

def test_consumer():
    client=pulsar.Client('pulsar://47.94.142.82:6650')

    consumer = client.subscribe('my-topic', 'my-subscription')




    consumer.topic()


    while True:
        msg = consumer.receive()
        try:
            print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
            # 确认已经成功处理消息
            consumer.acknowledge(msg)
        except:
            # 消息处理失败
            consumer.negative_acknowledge(msg)

    client.close()


if __name__=='__main__':
    test_consumer()