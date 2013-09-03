#!/usr/bin/env python

import re
import sys
import json
import time
import pickle
import struct
import socket
import thread
import urllib2
import argparse
from pprint import pprint

NODES = {}
CLUSTER_NAME = ''
STATUS = {'red': 0, 'yellow': 1, 'green': 2}
SHARD_STATE = {'CREATED': 0, 'RECOVERING': 1, 'STARTED': 2, 'RELOCATED': 3, 'CLOSED': 4}
HOST_IDX = -1

def get_es_host():
    global HOST_IDX
    HOST_IDX = (HOST_IDX + 1) % len(args.es) # round-robin
    return args.es[HOST_IDX]

def normalize(what):
    if not isinstance(what, (list, tuple)):
        return re.sub('\W+', '_',  what.strip().lower()).encode('utf-8')
    elif len(what) == 1:
        return normalize(what[0])
    else:
        return '%s.%s' % (normalize(what[0]), normalize(what[1:]))

def add_metric(metrics, prefix, stat, val, timestamp):
    if isinstance(val, bool):
        val = int(val)

    if prefix[-1] == 'translog' and stat == 'id':
        return
    elif isinstance(val, (int, long, float)) and stat != 'timestamp':
        metrics.append((normalize((prefix, stat)), (timestamp, val)))
    elif stat == 'status' and val in STATUS:
        metrics.append((normalize((prefix, stat)), (timestamp, STATUS[val])))
    elif stat == 'state' and val in SHARD_STATE:
        metrics.append((normalize((prefix, stat)), (timestamp, SHARD_STATE[val])))
        
def process_node_stats(metrics, prefix, stats):
    global CLUSTER_NAME
    CLUSTER_NAME = stats['cluster_name']
    for node_id in stats['nodes']:
        node_stats = stats['nodes'][node_id]
        NODES[node_id] = node_stats['name']
        process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, NODES[node_id]), node_stats)

def process_cluster_health(metrics, prefix, health):
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME), health)

def process_indices_status(metrics, prefix, status):
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), status['indices'])
    
def process_indices_stats(metrics, prefix, stats):
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices', '_all'), stats['_all'])
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), stats['indices'])
    
def process_segments_status(metrics, prefix, status):
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), status['indices'])
    
def process_section(timestamp, metrics, prefix, section):
    for stat in section:
        stat_val = section[stat]
        if 'timestamp' in section:
            timestamp = int(section['timestamp'] / 1000) # es has epoch in ms, graphite needs seconds

        if isinstance(stat_val, dict):
            process_section(timestamp, metrics, (prefix, stat), stat_val)
        elif isinstance(stat_val, list):
            if prefix[-1] == 'fs' and stat == 'data':
                for disk in stat_val:
                    mount = disk['mount']
                    process_section(timestamp, metrics, (prefix, stat, mount), disk)
            elif prefix[-1] == 'os' and stat == 'load_average':
                add_metric(metrics, prefix, (stat, '1min_avg'), stat_val[0], timestamp)
                add_metric(metrics, prefix, (stat, '5min_avg'), stat_val[1], timestamp)
                add_metric(metrics, prefix, (stat, '15min_avg'), stat_val[2], timestamp)
            elif prefix[-1] == 'shards' and re.match('\d+', stat) is not None:
                for shard in stat_val:
                    shard_node = NODES[shard['routing']['node']]
                    process_section(timestamp, metrics, (prefix, stat, shard_node), shard)
            else:
                for stat_idx, sub_stat_val in enumerate(stat_val):
                    if isinstance(sub_stat_val, dict):
                        process_section(timestamp, metrics, (prefix, stat, str(stat_idx)), sub_stat_val)
                    else:
                        add_metric(metrics, prefix, (stat, str(stat_idx)), sub_stat_val, timestamp)
        else:
            add_metric(metrics, prefix, stat, stat_val, timestamp)

def send_to_graphite(metrics):
    payload = pickle.dumps(metrics)
    header = struct.pack('!L', len(payload))
    sock = socket.socket()
    sock.connect((args.graphite_host, args.graphite_port))
    sock.sendall('%s%s' % (header, payload))
    sock.close()
 
def get_metrics():
    metrics = []

    node_stats_data = urllib2.urlopen('http://%s/_cluster/nodes/stats?all=true' % get_es_host()).read()
    node_stats = json.loads(node_stats_data)
    process_node_stats(metrics, args.prefix, node_stats)
 
    cluster_health_data = urllib2.urlopen('http://%s/_cluster/health?level=shards' % get_es_host()).read()
    cluster_health = json.loads(cluster_health_data)
    process_cluster_health(metrics, args.prefix, cluster_health)

    indices_status_data = urllib2.urlopen('http://%s/_status' % get_es_host()).read()
    indices_status = json.loads(indices_status_data)
    process_indices_status(metrics, args.prefix, indices_status)

    indices_stats_data = urllib2.urlopen('http://%s/_stats?all=true&level=shards' % get_es_host()).read()
    indices_stats = json.loads(indices_stats_data)
    process_indices_stats(metrics, args.prefix, indices_stats)
    
    segments_status_data = urllib2.urlopen('http://%s/_segments' % get_es_host()).read()
    segments_status = json.loads(segments_status_data)
    process_segments_status(metrics, args.prefix, segments_status)

    send_to_graphite(metrics)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send elasticsearch metrics to graphite')
    parser.add_argument('-p', '--prefix', default='es', help='graphite metric prefix. Default: %(default)s')
    parser.add_argument('-g', '--graphite-host', default='localhost', help='graphite hostname. Default: %(default)s')
    parser.add_argument('-o', '--graphite-port', default=2004, type=int, help='graphite pickle protocol port. Default: %(default)s')
    parser.add_argument('-i', '--interval', default=60, type=int, help='interval in seconds. Default: %(default)s')
    parser.add_argument('es', nargs='+', help='elasticsearch host:port', metavar='ES_HOST')
    args = parser.parse_args()
    while True:
        thread.start_new_thread(get_metrics, ())
        time.sleep(args.interval)
