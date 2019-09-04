import pulsar

def test_producer():
    client = pulsar.Client('pulsar://47.94.142.82:6650')

    producer = client.create_producer('my-topic')

    for i in range(10):
        producer.send(('Hello-%d' % i).encode('utf-8'))

    client.close()


if __name__=='__main__':
    test_producer()