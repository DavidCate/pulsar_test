import requests
import ujson

class PersistentTopic():
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

    def expiry_messages_on_all_subscriptions_of_topic(self,tenant,namespace,topic,expireTimeInSeconds):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}'.format(tenant=tenant,
                                                                                                     namespace=namespace,
                                                                                                     topic=topic,expireTimeInSeconds=expireTimeInSeconds)
        response = requests.delete(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def list_of_persistent_subscriptions_for_a_given_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscriptions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def get_last_commit_message_id_of_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/lastMessageId'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def skipping_messages_on_a_topic_subscription(self,tenant,namespace,topic,subName,numMessages):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName,
            numMessages=numMessages
            )
        response = requests.post(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def skip_all_messages_on_a_topic_subscription(self,tenant,namespace,topic,subName):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/skip_all'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName
        )
        response = requests.post(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg

    def Peek_nth_message_on_a_topic_subscription(self,tenant,namespace,topic,subName,messagePosition):
        SERVER_URL = self.__service_url + 'https://pulsar.apache.org/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName,
            messagePosition=messagePosition
        )
        response = requests.get(SERVER_URL)
        if response.status_code == 200:
            return ujson.loads(response.content.decode('utf-8'))
        else:
            msg = {
                'msg': 'http请求失败'
            }
            return msg