#!/usr/bin/env python

# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
import sys
import subprocess
from pyzabbix import ZabbixMetric, ZabbixSender
import urllib3
urllib3.disable_warnings()

# Try to establish a connection to elasticsearch.
try:
    es = Elasticsearch(
    ['127.0.0.1'],
    http_auth=('', ''),
    port='9200',
    use_ssl=True,
    verify_certs=False,
    #client_cert='/etc/ssl/certs/ssl-cert-snakeoil.pem',
    #client_key='/etc/ssl/private/ssl-cert-snakeoil.key

)

except Exception as e:
    print "Connection failed."
    sys.exit(1)

# Print error message in case of unsupported  metric.
def err_message(option, metric):

    print "%s metric is not under support for %s option." % (metric, option)
    sys.exit(1)

def cluster_health(metric):

    cluster_health = es.cluster.health(request_timeout=60)
    print cluster_health[metric]

def cluster_stats(metric):

    cluster_stats = es.cluster.stats(request_timeout=60)
    heap_max_value = cluster_stats['nodes']['jvm']['mem']['heap_max_in_bytes']
    heap_used_value = cluster_stats['nodes']['jvm']['mem']['heap_used_in_bytes']
    total_fs = cluster_stats['nodes']['fs']['total_in_bytes']
    available_fs = cluster_stats['nodes']['fs']['available_in_bytes']
    number_of_documents = cluster_stats['indices']['docs']['count']
    fielddata_evictions = cluster_stats['indices']['fielddata']['evictions']
    fielddata_memory_size_in_bytes = cluster_stats['indices']['fielddata']['memory_size_in_bytes']
    docs_count = cluster_stats['indices']['docs']['count']
    indices_count = cluster_stats['indices']['count']
    total_node =  cluster_stats['nodes']['count']['total']
    data_node =  cluster_stats['nodes']['count']['data']
    master_node =  cluster_stats['nodes']['count']['master']
    coordinating_node = cluster_stats['nodes']['count']['coordinating_only']

    metrics = [
        ZabbixMetric(sys.argv[3], 'es.cluster[heap_max_in_bytes]', heap_max_value),
        ZabbixMetric(sys.argv[3], 'es.cluster[heap_used_in_bytes]', heap_used_value),
        ZabbixMetric(sys.argv[3], 'es.cluster[total_fs]', total_fs),
        ZabbixMetric(sys.argv[3], 'es.cluster[available_fs]', available_fs),
        ZabbixMetric(sys.argv[3], 'es.cluster[number_of_documents]', number_of_documents),
        ZabbixMetric(sys.argv[3], 'es.cluster[fielddata_evictions]', fielddata_evictions),
        ZabbixMetric(sys.argv[3], 'es.cluster[fielddata_memory_size_in_bytes]', fielddata_memory_size_in_bytes),
        ZabbixMetric(sys.argv[3], 'es.cluster[docs_count]', docs_count),
        ZabbixMetric(sys.argv[3], 'es.cluster[indices_count]', indices_count),
        ZabbixMetric(sys.argv[3], 'es.cluster[total_node]', total_node),
        ZabbixMetric(sys.argv[3], 'es.cluster[data_node]', data_node),
        ZabbixMetric(sys.argv[3], 'es.cluster[master_node]', master_node),
        ZabbixMetric(sys.argv[3], 'es.cluster[coordinating_node]', coordinating_node),
    ]
    ZabbixSender(use_config=True).send(metrics)

def node_mem_stats(metric):

    node_stats = es.nodes.stats(node_id='_local', metric='jvm', request_timeout=60)
    node_id = node_stats['nodes'].keys()[0]

    if 'heap_used_percent' in metric:

        result = node_stats['nodes'][node_id]['jvm']['mem'][metric]
        print result
    else:
        if 'pool_young' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['young']
            size = result['used_in_bytes']
        elif 'pool_old' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['old']
            size = result['used_in_bytes']
        elif 'pool_survivor' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['survivor']
            size = result['used_in_bytes']
        else:
            result = node_stats['nodes'][node_id]['jvm']['mem']
            size = result[metric]
        print size


def node_stats(metric):

    node_stats = es.nodes.stats(node_id='_local', request_timeout=60)
    node_id = node_stats['nodes'].keys()[0]

    total_merge = node_stats['nodes'][node_id]['indices']['merges']['total_size_in_bytes']
    total_field = node_stats['nodes'][node_id]['indices']['fielddata']['memory_size_in_bytes']
    search_query_total = node_stats['nodes'][node_id]['indices']['search']['query_total']
    docs_count = node_stats['nodes'][node_id]['indices']['docs']['count']
    docs_deleted = node_stats['nodes'][node_id]['indices']['docs']['deleted']
    thread_bulk_rejected = node_stats['nodes'][node_id]['thread_pool']['bulk']['rejected']
    thread_bulk_completed = node_stats['nodes'][node_id]['thread_pool']['bulk']['completed']
    thread_get_rejected = node_stats['nodes'][node_id]['thread_pool']['get']['rejected']
    thread_get_completed = node_stats['nodes'][node_id]['thread_pool']['get']['completed']
    thread_index_completed = node_stats['nodes'][node_id]['thread_pool']['index']['completed']
    thread_listener_completed = node_stats['nodes'][node_id]['thread_pool']['listener']['completed']
    #thread_percolate_completed = node_stats['nodes'][node_id]['thread_pool']['percolate']['completed']
    thread_refresh_completed = node_stats['nodes'][node_id]['thread_pool']['refresh']['completed']
    thread_search_completed = node_stats['nodes'][node_id]['thread_pool']['search']['completed']
    thread_snapshot_completed = node_stats['nodes'][node_id]['thread_pool']['snapshot']['completed']
    #thread_suggest_completed = node_stats['nodes'][node_id]['thread_pool']['suggest']['completed']
    thread_warmer_completed = node_stats['nodes'][node_id]['thread_pool']['warmer']['completed']

    metrics = [
        ZabbixMetric(sys.argv[3], 'es.node[total_merges_mem]', total_merge),
        ZabbixMetric(sys.argv[3], 'es.node[total_field_data_mem]', total_field),
        ZabbixMetric(sys.argv[3], 'es.node[search_query_total]', search_query_total),
        ZabbixMetric(sys.argv[3], 'es.node[docs_count]', docs_count),
        ZabbixMetric(sys.argv[3], 'es.node[docs_deleted]', docs_deleted),
        ZabbixMetric(sys.argv[3], 'es.node[thread_bulk_rejected]', thread_bulk_rejected),
        ZabbixMetric(sys.argv[3], 'es.node[thread_bulk_completed]', thread_bulk_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_get_rejected]', thread_get_rejected),
        ZabbixMetric(sys.argv[3], 'es.node[thread_get_completed]', thread_get_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_index_completed]', thread_index_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_listener_completed]', thread_listener_completed),
        #ZabbixMetric(sys.argv[3], 'es.node[thread_percolate_completed]', thread_percolate_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_refresh_completed]', thread_refresh_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_search_completed]', thread_search_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_snapshot_completed]', thread_snapshot_completed),
        #ZabbixMetric(sys.argv[3], 'es.node[thread_suggest_completed]', thread_suggest_completed),
        ZabbixMetric(sys.argv[3], 'es.node[thread_warmer_completed]', thread_warmer_completed),
    ]
    ZabbixSender(use_config=True).send(metrics)


def indice_stats(metric):

    indices_stats = es.indices.stats(metric='_all', filter_path='_all.total.*', request_timeout=60)
    flush_total_time_in_millis = indices_stats['_all']['total']['flush']['total_time_in_millis']
    flush_total = indices_stats['_all']['total']['flush']['total']
    get_time_in_millis = indices_stats['_all']['total']['get']['time_in_millis']
    get_total = indices_stats['_all']['total']['get']['total']
    merges_total_time_in_millis = indices_stats['_all']['total']['merges']['total_time_in_millis']
    merges_total = indices_stats['_all']['total']['merges']['total']
    indexing_total_time_in_millis = indices_stats['_all']['total']['indexing']['index_time_in_millis']
    indexing_total = indices_stats['_all']['total']['indexing']['index_total']
    refresh_total_time_in_millis = indices_stats['_all']['total']['refresh']['total_time_in_millis']
    refresh_total = indices_stats['_all']['total']['refresh']['total']
    search_query_time_in_millis = indices_stats['_all']['total']['search']['query_time_in_millis']
    search_query_total = indices_stats['_all']['total']['search']['query_total']

    metrics = [
        ZabbixMetric(sys.argv[3], 'es.indice[flush_total_time_in_millis]', flush_total_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[flush_total]', flush_total),
        ZabbixMetric(sys.argv[3], 'es.indice[get_time_in_millis]', get_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[get_total]', get_total),
        ZabbixMetric(sys.argv[3], 'es.indice[merges_total_time_in_millis]', merges_total_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[merges_total]', merges_total),
        ZabbixMetric(sys.argv[3], 'es.indice[indexing_time_in_millis]', indexing_total_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[indexing_total]', indexing_total),
        ZabbixMetric(sys.argv[3], 'es.indice[refresh_total_time_in_millis]', refresh_total_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[refresh_total]', refresh_total),
        ZabbixMetric(sys.argv[3], 'es.indice[search_query_time_in_millis]', search_query_time_in_millis),
        ZabbixMetric(sys.argv[3], 'es.indice[search_query_total]', search_query_total),
    ]
    ZabbixSender(use_config=True).send(metrics)

# Definition of checks

cluster_checks = {'active_primary_shards': cluster_health,
                  'active_shards': cluster_health,
                  'number_of_pending_tasks': cluster_health,
                  'relocating_shards': cluster_health,
                  'status': cluster_health,
                  'unassigned_shards': cluster_health,
                  'stats': cluster_stats}

node_checks = {'heap_pool_young_gen_mem': node_mem_stats,
               'heap_pool_old_gen_mem': node_mem_stats,
               'heap_pool_survivor_gen_mem': node_mem_stats,
               'heap_max_in_bytes': node_mem_stats,
               'heap_used_in_bytes': node_mem_stats,
               'heap_used_percent': node_mem_stats,
               'stats': node_stats}

indice_checks = {'stats': indice_stats}

if __name__ == '__main__':

    #if len(sys.argv) < 4:
    #    print "Positional arguments count should be 4."
    #    sys.exit(2)

    try:
        if sys.argv[1] == 'cluster':
            cluster_checks.get(sys.argv[2])(sys.argv[2])

        if sys.argv[1] == 'node':
            node_checks.get(sys.argv[2])(sys.argv[2])

        if sys.argv[1] == 'indice':
            indice_checks.get(sys.argv[2])(sys.argv[2])

    except TypeError as e:
        #err_message(sys.argv[1], sys.argv[2])
        print e
