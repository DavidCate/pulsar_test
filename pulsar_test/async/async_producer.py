import pulsar

def test():
    client = pulsar.Client('pulsar://localhost:6650')
    producer = client.create_producer(
                    'my-topic',
                    block_if_queue_full=True,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=10
                )

    url=''
    client.create_producer(
        topic='',
        producer_name='fdf',
        schema=pulsar.schema.JsonSchema,

    )

    def send_callback(res, msg):
        print('Message published res=%s', res)

    while True:
        producer.send_async(('Hello-%d' % i).encode('utf-8'), send_callback)

    client.close()