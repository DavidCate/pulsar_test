import mango_pulsar_client
import pulsar

def test():
    SERVICE_URL = "pulsar://pulsar.aimango.net:6650"
    REST_URL = "http://pulsar.aimango.net:8080"
    cli=pulsar.Client(SERVICE_URL)
    produ=cli.create_producer('persistent://public/default/liuchaozhi','xxx')
    produ.send('xxx'.encode('utf-8'))
    pulsar_client = mango_pulsar_client.MangoPulsarClient(SERVICE_URL, REST_URL)
    pro=pulsar_client.create_producer('persistent://public/default/liuchaozhi')
    pro.send('xxx'.encode('utf-8'))
    topics=pulsar_client.admin.persistent_topic.get_the_list_of_topics_under_namespace('public','default')
    print(topics)
    topic='persistent://public/default/xxxx'
    res=pulsar_client.admin.persistent_topic.create_non_partitioned_topic('public','default','xxxx')
    print(res['content'])
    res=pulsar_client.admin.clusters.get_the_list_of_all_the_Pulsar_clusters()
    print(res)
    producer=pulsar_client.create_producer(topic,'xxx-producer')
    producer.send('xxxx'.encode('utf-8'))
    producer.flush()
    pulsar_client.close()


def test1():
    SERVICE_URL = "pulsar://pulsar.aimango.net:6650"
    REST_URL = "http://pulsar.aimango.net:8080"
    # cli = pulsar.Client(SERVICE_URL)
    cli=mango_pulsar_client.MangoPulsarClient(SERVICE_URL,REST_URL)
    res=cli.admin.persistent_topic.peek_nth_message_on_a_topic_subscription('public','default','xiongzhengxing','aaa',2)
    produ = cli.create_producer('persistent://public/default/xiongzhengxing', 'xxx')
    produ.send('dsfasdf'.encode('utf-8'))
    lastmsgid=cli.admin.persistent_topic.get_last_commit_message_id_of_topic('public','default','xiongzhengxing')
    id=lastmsgid['content']['entryId']
    print(id)
    print(lastmsgid)
    id=produ.last_sequence_id()
    print(id)
    print(res)
    # produ = cli.create_producer('persistent://public/default/xiongzhengxing', 'xxx')
    # for index in range(10):
    #     produ.send(str(index).encode('utf-8'))
    #     print('msgid:{id}'.format(id=produ.last_sequence_id()))
    # sub=cli.subscribe('persistent://public/default/xiongzhengxing','aaa')
    # res=sub.receive()
    # print(res.value())
    # sub.acknowledge(res)
    # print(sub.seek())
    # for index in range(10):
    #     produ.send(str(index).encode('utf-8'))
    #     print('msgid:{id}'.format(id=produ.last_sequence_id()))







if __name__ == '__main__':
    test1()