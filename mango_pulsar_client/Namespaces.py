import requests


class Namespaces():
    def __init__(self,service_url):
        self.__service_url=service_url

    '''
    
    '''
    def get_all_namespaces_that_are_grouped_by_given_anti_affinity_group_in_a_given_cluster(self,cluster,group,tenant):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{cluster}/antiAffinity/{group}?tenant={tenant}'.format(cluster=cluster,group=group,tenant=tenant)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    '''
    
    '''
    def get_the_bookie_affinity_group_from_namespace_local_policy(self,property,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{property}/{namespace}/persistence/bookieAffinity'.format(property=property,namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_the_bookie_affinity_group_from_namespace_local_policy(self,property,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{property}/{namespace}/persistence/bookieAffinity'.format(
            property=property, namespace=namespace)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    '''
    获取某个租户下的所有namespace
    '''
    def get_the_list_of_all_the_namespaces_for_a_certain_tenant(self,tenant):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}'.format(
            tenant=tenant)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_dump_all_the_policies_specified_for_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}'.format(
            tenant=tenant,namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg


    def creates_a_new_namespace_with_the_specified_policies(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}'.format(
            tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_namespace_and_all_the_topics_under_it(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}'.format(
            tenant=tenant, namespace=namespace)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_anti_affinity_group_of_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/antiAffinity'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_anti_affinity_group_for_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/antiAffinity'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def remove_anti_affinity_group_of_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/antiAffinity'.format(
            tenant=tenant, namespace=namespace)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_a_backlog_quota_for_all_the_topics_on_a_namespace(self,tenant,namespace,backlogQuotaType):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/backlogQuota?backlogQuotaType={backlogQuotaType}'.format(
            tenant=tenant, namespace=namespace,backlogQuotaType=backlogQuotaType)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def remove_a_backlog_quota_policy_from_a_namespace(self,tenant,namespace,backlogQuotaType):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/backlogQuota?backlogQuotaType={backlogQuotaType}'.format(
            tenant=tenant, namespace=namespace, backlogQuotaType=backlogQuotaType)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_backlog_quota_map_on_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/backlogQuotaMap'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_bundles_split_data(self,tenant,namespace):
        SERVER_URL = self.__service_url + 'https://pulsar.apache.org/admin/v2/namespaces/{tenant}/{namespace}/bundles'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def clear_backlog_for_all_topics_on_a_namespace(self,tenant,namespace,authoritative):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/clearBacklog?authoritative={authoritative}'.format(
            tenant=tenant, namespace=namespace,authoritative=authoritative)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def clear_backlog_for_a_given_subscription_on_all_topics_on_a_namespace(self,tenant,namespace,subscription,authoritative):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/clearBacklog/{subscription}?authoritative={authoritative}'.format(
            tenant=tenant, namespace=namespace, subscription=subscription,authoritative=authoritative)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def maximum_number_of_uncompacted_bytes_in_topics_before_compaction_is_triggered(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/compactionThreshold'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_maximum_number_of_uncompacted_bytes_in_a_topic_before_compaction_is_triggered(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/compactionThreshold'.format(
            tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def enable_or_disable_broker_side_deduplication_for_all_topics_in_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/deduplication'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_dispatch_rate_configured_for_the_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/dispatchRate'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_dispatch_rate_throttling_for_all_topics_of_the_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/dispatchRate'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def message_encryption_is_required_or_not_for_all_topics_in_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/encryptionRequired'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_maxConsumersPerSubscription_config_on_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/maxConsumersPerSubscription'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_maxConsumersPerTopic_configuration_on_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/maxConsumersPerTopic'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_maxProducersPerTopic_config_on_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/maxProducersPerTopic'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_maxProducersPerTopic_configuration_on_a_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/maxProducersPerTopic'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_message_TTL_for_the_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/messageTTL'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_message_TTL_in_seconds_for_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/messageTTL'.format(
            tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def number_of_milliseconds_to_wait_before_deleting_a_ledger_segment_which_has_been_offloaded_from_the_Pulsar_cluster_local_storage_(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/offloadDeletionLagMs'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_number_of_milliseconds_to_wait_before_deleting_a_ledger_segment_which_has_been_offloaded_from_the_Pulsar_cluster_local_storage(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/offloadDeletionLagMs'.format(
            tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def clear_the_namespace_configured_offload_deletion_lag(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/offloadDeletionLagMs'.format(
            tenant=tenant, namespace=namespace)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def maximum_number_of_bytes_stored_on_the_pulsar_cluster_for_a_topic(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/offloadDeletionLagMs'.format(
            tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_maximum_number_of_bytes_stored_on_the_pulsar_cluster_for_a_topic(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/offloadThreshold'.format(
            tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def grant_a_new_permission_to_a_role_on_a_namespace(self,tenant,namespace,role):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/permissions/{role}'.format(
            tenant=tenant, namespace=namespace,role=role)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def revoke_all_permissions_to_a_role_on_a_namespace(self,tenant, namespace, role):
        """
        撤销角色在某个命名空间的所有的权限
        :param tenant:
        :param namespace:
        :param role:
        :return:
        """
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/permissions/{role}'\
            .format(tenant=tenant, namespace=namespace, role=role)
        response = requests.delete(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_persistence_configuration_for_a_namespace(self, tenant, namespace):
        """
        获取命名空间的持久性配置
        :param tenant:
        :param namespace:
        :return:
        """
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/persistence'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_the_persistence_configuration_or_all_the_topics_on_a_namespace(self,tenant,namespace):
        """
        为命名空间上的所有主题设置持久性配置
        :param tenant:
        :param namespace:
        :return:
        """
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/persistence'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_the_bookie_affinity_group_to_namespace_persistent_policy(self, tenant, namespace):
        """
        将Bookie关联组设置为命名空间持久策略
        :param tenant:
        :param namespace:
        :return:
        """
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/persistence/bookieAffinity'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_replication_clusters_for_a_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/replication'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_the_replication_clusters_for_a_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/replication'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_replicator_dispatch_rate_configured_for_the_namespace(self,tenant,namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/replicatorDispatchRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_replicator_dispatch_rate_throttling_for_all_topics_of_the_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/replicatorDispatchRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_retention_config_on_a_namespace(self,tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/retention'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_retention_configuration_on_a_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/retention'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def the_strategy_used_to_check_the_compatibility_of_new_schemas_provided_by_producers_before_automatically_updating_the_schema(self,tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def update_the_strategy_used_to_check_the_compatibility_of_new_schemas_provided_by_producers_before_automatically_updating_the_schema(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/schemaAutoUpdateCompatibilityStrategy'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_schema_validation_enforced_flag_for_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/schemaValidationEnforced'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_schema_validation_enforced_flag_on_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/schemaValidationEnforced'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_subscribe_rate_configured_for_the_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/subscribeRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def set_subscribe_rate_throttling_for_all_topics_of_the_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/subscribeRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def  set_a_subscription_auth_mode_for_all_the_topics_on_a_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/subscriptionAuthMode'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_subscription_dispatch_rate_configured_for_the_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/subscriptionDispatchRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg


    def set_subscription_dispatch_rate_throttling_for_all_topics_of_the_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/subscriptionDispatchRate'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def get_the_list_of_all_the_topics_under_a_certain_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/topics'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.get(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def unload_namespace(self, tenant, namespace):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/unload'\
            .format(tenant=tenant, namespace=namespace)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def unsubscribes_the_given_subscription_on_all_topics_ona_namespace(self, tenant, namespace, subscription):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/unsubscribe/{subscription}'\
            .format(tenant=tenant, namespace=namespace, subscription=subscription)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def delete_a_namespace_bundle_and_all_the_topics_under_it(self, tenant, namespace, bundle):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}'\
            .format(tenant=tenant, namespace=namespace, bundle=bundle)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def clear_backlog_for_all_topics_on_a_namespace_bundle(self, tenant, namespace, bundle):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}/clearBacklog'\
            .format(tenant=tenant, namespace=namespace, bundle=bundle)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def clear_backlog_for_a_given_subscription_on_all_topics_on_a_namespace_bundle(self, tenant, namespace, bundle, subscription):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}/clearBacklog/{subscription}'\
            .format(tenant=tenant, namespace=namespace,bundle=bundle, subscription=subscription)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def split_a_namespace_bundle(self, tenant, namespace, bundle):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}/split'\
            .format(tenant=tenant,namespace=namespace,bundle=bundle)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def unload_a_namespace_bundle(self,tenant, namespace, bundle):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}/unload' \
            .format(tenant=tenant, namespace=namespace, bundle=bundle)
        response = requests.put(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg

    def unsubscribes_the_given_subscription_on_all_topics_on_a_namespace_bundle(self, tenant, namespace, bundle, subscription):
        SERVER_URL = self.__service_url + '/admin/v2/namespaces/{tenant}/{namespace}/{bundle}/unsubscribe/{subscription}'\
            .format(tenant=tenant, namespace=namespace, bundle=bundle, subscription=subscription)
        response = requests.post(SERVER_URL)
        msg = {
            'http_status': response.status_code,
            'content': response.content.decode('utf-8')
        }
        return msg