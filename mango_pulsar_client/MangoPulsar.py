import pulsar
from pulsar import *
from mango_pulsar_client.MangoPulsarAdmin import MangoPulsarAdmin

class MangoPulsarClient(pulsar.Client):
    admin=None

    def __init__(self,service_url,
                 http_service_url,
                 authentication=None,
                 operation_timeout_seconds=30,
                 io_threads=1,
                 message_listener_threads=1,
                 concurrent_lookup_requests=50000,
                 log_conf_file_path=None,
                 use_tls=False,
                 tls_trust_certs_file_path=None,
                 tls_allow_insecure_connection=False,
                 tls_validate_hostname=False):
        super(MangoPulsarClient,self).__init__(service_url,authentication,
                                               operation_timeout_seconds,
                                               io_threads,message_listener_threads,
                                               concurrent_lookup_requests,
                                               log_conf_file_path,use_tls,
                                               tls_trust_certs_file_path,
                                               tls_allow_insecure_connection,
                                               tls_validate_hostname)
        self.admin=MangoPulsarAdmin(http_service_url)

    def create_producer(self, topic, producer_name=None, schema=schema.BytesSchema(), initial_sequence_id=None,
                        send_timeout_millis=30000, compression_type=CompressionType.NONE, max_pending_messages=1000,
                        max_pending_messages_across_partitions=50000, block_if_queue_full=False, batching_enabled=False,
                        batching_max_messages=1000, batching_max_allowed_size_in_bytes=128 * 1024,
                        batching_max_publish_delay_ms=10,
                        message_routing_mode=PartitionsRoutingMode.RoundRobinDistribution, properties=None):
        return super().create_producer(topic, producer_name, schema, initial_sequence_id, send_timeout_millis,
                                       compression_type, max_pending_messages, max_pending_messages_across_partitions,
                                       block_if_queue_full, batching_enabled, batching_max_messages,
                                       batching_max_allowed_size_in_bytes, batching_max_publish_delay_ms,
                                       message_routing_mode, properties)

    def subscribe(self, topic, subscription_name, consumer_type=ConsumerType.Exclusive, schema=schema.BytesSchema(),
                  message_listener=None, receiver_queue_size=1000,
                  max_total_receiver_queue_size_across_partitions=50000, consumer_name=None,
                  unacked_messages_timeout_ms=None, broker_consumer_stats_cache_time_ms=30000,
                  negative_ack_redelivery_delay_ms=60000, is_read_compacted=False, properties=None,
                  pattern_auto_discovery_period=60, initial_position=InitialPosition.Latest):
        return super().subscribe(topic, subscription_name, consumer_type, schema, message_listener, receiver_queue_size,
                                 max_total_receiver_queue_size_across_partitions, consumer_name,
                                 unacked_messages_timeout_ms, broker_consumer_stats_cache_time_ms,
                                 negative_ack_redelivery_delay_ms, is_read_compacted, properties,
                                 pattern_auto_discovery_period, initial_position)

    def create_reader(self, topic, start_message_id, schema=schema.BytesSchema(), reader_listener=None,
                      receiver_queue_size=1000, reader_name=None, subscription_role_prefix=None):
        return super().create_reader(topic, start_message_id, schema, reader_listener, receiver_queue_size, reader_name,
                                     subscription_role_prefix)

    def get_topic_partitions(self, topic):
        return super().get_topic_partitions(topic)

    def close(self):
        super().close()