import requests
import ujson
from mango_pulsar.PersistentTopic import PersistentTopic
from mango_pulsar.Namespaces import Namespaces
from mango_pulsar.Tenants import Tenants
from mango_pulsar.Bookies import Bookies
from mango_pulsar.Brokers import Brokers
from mango_pulsar.BrokerStats import BrokerStats
from mango_pulsar.Clusters import Clusters
from mango_pulsar.NonPersistentTopic import NonPersistentTopic
from mango_pulsar.ResourceQuotas import ResourceQuotas
from mango_pulsar.Schemas import Schemas

class MangoPulsarAdmin():
    def __init__(self,service_url):
        self.__service_url=service_url
        self.persistent_topic=PersistentTopic(self.__service_url)
        self.namespaces=Namespaces(self.__service_url)
        self.tenants=Tenants(self.__service_url)
        self.bookies=Brokers
        self.brokerstats=BrokerStats
        self.bookies=Bookies
        self.clusters=Clusters
        self.nonpersistenttopic=NonPersistentTopic
        self.resourcequotas=ResourceQuotas
        self.schemas=Schemas


