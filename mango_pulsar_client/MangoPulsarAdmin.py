import requests
import ujson
from mango_pulsar_client.PersistentTopic import PersistentTopic
from mango_pulsar_client.Namespaces import Namespaces
from mango_pulsar_client.Tenants import Tenants
from mango_pulsar_client.Bookies import Bookies
from mango_pulsar_client.Brokers import Brokers
from mango_pulsar_client.BrokerStats import BrokerStats
from mango_pulsar_client.Clusters import Clusters
from mango_pulsar_client.NonPersistentTopic import NonPersistentTopic
from mango_pulsar_client.ResourceQuotas import ResourceQuotas
from mango_pulsar_client.Schemas import Schemas

class MangoPulsarAdmin():
    def __init__(self,service_url):
        self.__service_url=service_url
        self.persistent_topic=PersistentTopic(self.__service_url)
        self.namespaces=Namespaces(self.__service_url)
        self.tenants=Tenants(self.__service_url)
        self.bookies=Brokers(self.__service_url)
        self.brokerstats=BrokerStats(self.__service_url)
        self.bookies=Bookies(self.__service_url)
        self.clusters=Clusters(self.__service_url)
        self.nonpersistenttopic=NonPersistentTopic(self.__service_url)
        self.resourcequotas=ResourceQuotas(self.__service_url)
        self.schemas=Schemas(self.__service_url)


