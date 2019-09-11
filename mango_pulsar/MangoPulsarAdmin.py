import requests
import ujson
from mango_pulsar.PersistentTopic import PersistentTopic

class MangoPulsarAdmin():
    def __init__(self,service_url):
        self.__service_url=service_url
        self.persistent_topic=PersistentTopic(self.__service_url)



