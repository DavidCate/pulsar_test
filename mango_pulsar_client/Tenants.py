import requests

class Tenants():
    def __init__(self,service_url):
        self.__service_url=service_url

    def get_the_list_of_existing_tenants(self):
        SERVER_URL = self.__service_url + '/admin/v2/tenants'
        response = requests.get(SERVER_URL)
        msg={
            'http_status':response.status_code,
            'content':response.content.decode('utf-8')
        }
        return msg

    def get_the_admin_configuration_for_a_tenant(self,tenant):
        SERVER_URL = self.__service_url + '/admin/v2/tenants/{tenant}'.format(tenant=tenant)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def update_the_admins_for_a_tenant(self,tenant,adminRoles:list,allowedClusters:list):
        SERVER_URL = self.__service_url + '/admin/v2/tenants/{tenant}'.format(tenant=tenant)
        REQUEST_BODY={
            'adminRoles':adminRoles,
            'allowedClusters':allowedClusters
        }
        response = requests.post(SERVER_URL,json=REQUEST_BODY)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def create_a_new_tenant(self,tenant,adminRoles:list,allowedClusters:list):
        SERVER_URL = self.__service_url + '/admin/v2/tenants/{tenant}'.format(tenant=tenant)
        REQUEST_BODY = {
            'adminRoles': adminRoles,
            'allowedClusters': allowedClusters
        }
        response = requests.put(SERVER_URL, json=REQUEST_BODY)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_tenant_and_all_namespaces_and_topics_under_it(self,tenant):
        SERVER_URL = self.__service_url + '/admin/v2/tenants/{tenant}'.format(tenant=tenant)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg