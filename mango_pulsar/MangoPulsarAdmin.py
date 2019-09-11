import requests
import ujson
from mango_pulsar.PersistentTopic import PersistentTopic
from mango_pulsar.Namespaces import Namespaces
from mango_pulsar.Tenants import Tenants

class MangoPulsarAdmin():
    def __init__(self,service_url):
        self.__service_url=service_url
        self.persistent_topic=PersistentTopic(self.__service_url)
        self.namespaces=Namespaces(self.__service_url)
        self.tenants=Tenants(self.__service_url)



