import requests
import ujson

class PersistentTopic():
    def __init__(self,service_url):
        self.__service_url=service_url

    def get_the_list_of_topics_under_namespace(self,tenant,namespace):
        SERVER_URL= self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}'.format(tenant=tenant,namespace=namespace)
        response=requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg


    def get_thie_list_of_partitioned_topics_under_namespace(self,tenant,namespace):
        SERVER_URL=self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}/partitioned'.format(tenant=tenant,namespace=namespace)
        response=requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def create_non_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}'.format(tenant=tenant,namespace=namespace,topic=topic)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_topic(self,tenant,namespace,topic):
        SERVER_URL=self.__service_url+'/admin/v2/persistent/{tenant}/{namespace}/{topic}'.format(tenant=tenant,namespace=namespace,topic=topic)
        response=requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def expiry_messages_on_all_subscriptions_of_topic(self,tenant,namespace,topic,expireTimeInSeconds):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}'.format(tenant=tenant,
                                                                                                     namespace=namespace,
                                                                                                     topic=topic,expireTimeInSeconds=expireTimeInSeconds)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg


    def get_estimated_backlog_for_offline_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/backlog'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_status_of_a_compaction_operation_for_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def trigger_a_compaction_operation_on_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_internal_stats_for_the_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/internal-info'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_internal_stats_for_the_topic_with_authoritative(self,tenant,namespace,topic,authoritative):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/internalStats?authoritative={authoritative}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            authoritative=authoritative
        )
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_last_commit_message_id_of_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/lastMessageId'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def return_the_last_commit_message_id_of_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/lastMessageId'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def offload_a_prefix_of_a_topic_to_long_term_storage(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/offload'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def offload_a_prefix_of_a_topic_to_long_term_storage(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/offload'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_stats_for_the_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-stats'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_partitioned_topic_metadata(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def increment_partitons_of_an_existing_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def create_a_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_partitioned_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_permissions_on_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/permissions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def grant_a_new_permission_to_a_role_on_a_single_topic(self,tenant,namespace,topic,role):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/permissions/{role}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            role=role
        )
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def revoke_permissions_on_a_topic(self,tenant,namespace,topic,role):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/permissions/{role}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            role=role
        )
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_stats_for_the_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/stats'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic
        )
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_subscription(self,tenant,namespace,topic,subName):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic
        )
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def expiry_messages_on_a_topic_subscription(self,tenant,namespace,topic,subName,expireTimeInSeconds):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName,
            expireTimeInSeconds=expireTimeInSeconds
        )
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def peek_nth_message_on_a_topic_subscription(self,tenant,namespace,topic,subName,messagePosition):
        SERVER_URL = self.__service_url + 'https://pulsar.apache.org/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName,
            messagePosition=messagePosition
        )
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    ####
    def reset_subscription_to_message_position_closest_to_given_position(self,tenant,namespace,topic,subName):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName
        )
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def reset_subscription_to_message_position_closest_to_absolute_timestamp(self,tenant,namespace,topic,subName,timestamp):
        SERVER_URL = self.__service_url + 'https://pulsar.apache.org/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subName=subName,
            timestamp=timestamp
        )
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
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
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
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
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def reset_subscription_to_message_position_closest_to_given_position(self,tenant,namespace,topic,subscriptionName):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            subscriptionName=subscriptionName
        )
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_list_of_persistent_subscriptions_for_a_given_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscriptions'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def terminate_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/terminate'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def unload_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/persistent/{tenant}/{namespace}/{topic}/unload'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg









