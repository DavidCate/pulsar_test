import requests

class Clusters():
    def __init__(self,service_url):
        self.__service_url=service_url

    def get_the_list_of_all_the_Pulsar_clusters(self,):
        SERVER_URL = self.__service_url + '/admin/v2/clusters'
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg


    def get_the_namespace_isolation_policies_assigned_to_the_cluster(self,cluster:str):
        SERVER_URL = self.__service_url + '/admin/v2/clusters/{cluster}/namespaceIsolationPolicies'.format(cluster=cluster)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_list_of_brokers_with_namespace_isolation_policies_attached_to_them(self,cluster:str):
        SERVER_URL = self.__service_url + '/admin/v2/clusters/{cluster}/namespaceIsolationPolicies/brokers'.format(
            cluster=cluster)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_a_broker_with_namespace_isolation_policies_attached_to_it(self,cluster,broker):
        SERVER_URL = self.__service_url + '/admin/v2/clusters/{cluster}/namespaceIsolationPolicies/brokers/{broker}'.format(
            cluster=cluster,broker=broker)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_single_namespace_isolation_policy_assigned_to_the_cluster(self,cluster,policyName):
        SERVER_URL = self.__service_url + '/admin/v2/clusters/{cluster}/namespaceIsolationPolicies/{policyName}'.format(
            cluster=cluster, policyName=policyName)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg
