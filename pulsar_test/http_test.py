import requests
from multiprocessing import Pool
import os
import time

url='http://pulsar.aimango.net:8080/admin/v2/persistent/public/default'


def main():
    print('Parent process %s.' % os.getpid())
    p = Pool(1000)
    for i in range(1000):
        p.apply_async(send_request())
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

def send_request():
    print('Run task {pid}'.format(pid=os.getpid()))
    response = requests.get(url)
    if response.status_code == 200:
        print(response.content.decode('utf-8'))
    else:
        print('请求失败')


if __name__ == '__main__':
    main()