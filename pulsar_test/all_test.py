import pulsar
import datetime

def call(a,b):
    print('消息已经到达broker')
    print(a)
    print(b)


def my_message_listenner(consummer,message):
    consummer


def test():

    client=pulsar.Client('pulsar://47.94.142.82:6650')

    #一个topic 就是一个日志
    # 一个topic 可以被分成很多partition （分区） 可以看做为 一段一段日志
    # 这些分区 由broker提供服务，默认一个broker 服务一个分区 如果 分区数大于 broker数量，broker将会服务多个分区topic
    # 配置生产者时 可以 给生产者配置路由模式 有三种：RoundRobinPartition，SinglePartition，CustomPartition，默认为第一个
    # 可以在生产者生产消息时 提供路由key 提高查找效率
    #默认 消息的路由模式 round-robin
    producer= client.create_producer('topic-1','liuchaozhi')
    partitions=client.get_topic_partitions('topic-1')

    print(partitions)
    print(producer.topic())
    producer.send('xxxx'.encode('utf-8'))

    producer.flush()
    print(producer.last_sequence_id())
    print(producer.producer_name())
    producer.send_async('sdfasdfasdfdsf'.encode('utf-8'),callback=call)
    print(producer.last_sequence_id())
    producer.close()



    #消费者示例
    #subscribe 函数配置消费者消费 模式
    #schema限制topic 中的消息的数据类型  json  , bytes,AvroSchemaJsonSchema  默认是bytes 的

    #消费者还可以设置 message_listener(consumer, message)
    #client.subscribe('topic','sub_name',message_listener=my_message_listener)
    # def my_message_listener(consumer,message):
    #   consumer.acknowledge()
    #   ...
    #注 若设置了 message_listener  不允许再使用consumer.receive()

    consumer_type= pulsar.ConsumerType.Exclusive

    consumer = client.subscribe('topic-1','sub-1',consumer_type=consumer_type,schema=pulsar.schema.BytesSchema())
    print(consumer.topic())


    msg=consumer.receive()
    #bytes数据
    print(msg.data())
    #反序列化之后的数据
    print(msg.value())
    #消息的分区key
    print(msg.partition_key())
    consumer.acknowledge(msg)

    # reader=client.create_reader()

    # client.get_topic_partitions()

if __name__=='__main__':
    test()