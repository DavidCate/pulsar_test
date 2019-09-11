import requests
import ujson

class MangoPulsarAdmin():
    def __init__(self,service_url):
        self.__service_url=service_url

    def create_non_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}'.format(tenant=tenant,namespace=namespace,topic=topic)
        response = requests.put(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def list_topics_under_namespace(self,tenant,namespace):
        SERVER_URL= self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}'.format(tenant=tenant,namespace=namespace)
        response=requests.get(SERVER_URL)
        if response.status_code==200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg={
                'msg':'http请求失败'
            }
            return msg

    def list_partitioned_topics_under_namespace(self,tenant,namespace):
        SERVER_URL=self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}/partitioned'.format(tenant=tenant,namespace=namespace)
        response=requests.get(SERVER_URL)
        if response.status_code==200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def delete(self,tenant,namespace,topic):
        SERVER_URL=self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}/{topic}'.format(tenant=tenant,namespace=namespace,topic=topic)
        response=requests.delete(SERVER_URL)
        if response.status_code==200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg