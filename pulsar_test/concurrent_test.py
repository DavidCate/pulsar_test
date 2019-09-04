from multiprocessing import Process
from multiprocessing import Pool
import pulsar
client=pulsar.Client('pulsar://127.0.0.1:6650')

def sub_method(i):
    producer=client.create_producer('topic-'+str(i),'producer-'+str(i))
    print('创建producer-{i}'.format(i=str(i)))
    producer.send(str(i).encode('utf-8'))
    print('发送了消息：{msg}'.format(msg=str(i)))

def main():
    pool=Pool(5000)
    for i in range(5000):
        pool.apply_async(sub_method,args=(i,))
    pool.close()
    pool.join()
    print('all subprocesses done')

if __name__=='__main__':
    main()