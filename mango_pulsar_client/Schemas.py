import requests

class Schemas():
    def __init__(self,service_url):
        self.__service_url=service_url

    def test_the_schema_compatibility(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/compatibility'.format(tenant=tenant,
        namespace=namespace,topic=topic)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_schema_of_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/schema'.format(
            tenant=tenant,
            namespace=namespace, topic=topic)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def update_the_schema_of_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/schema'.format(
            tenant=tenant,
            namespace=namespace, topic=topic)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_the_schema_of_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/schema'.format(
            tenant=tenant,
            namespace=namespace, topic=topic)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_schema_of_a_topic_at_a_given_version(self,tenant,namespace,topic,version):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/schema/{version}'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic,
            version=version
        )
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_all_schemas_of_a_topic(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/schemas'.format(
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

    def get_the_version_of_the_schema(self,tenant,namespace,topic):
        SERVER_URL = self.__service_url + '/admin/v2/schemas/{tenant}/{namespace}/{topic}/version'.format(
            tenant=tenant,
            namespace=namespace,
            topic=topic
        )
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg