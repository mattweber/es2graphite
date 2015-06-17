#!/usr/bin/env python

import re
import sys
import json
import logging
import logging.handlers
import time
import pickle
import struct
import socket
import thread
import urllib2
import argparse
from datetime import datetime

NODES = {}
CLUSTER_NAME = ''
STATUS = {'red': 0, 'yellow': 1, 'green': 2}
SHARD_STATE = {'CREATED': 0, 'RECOVERING': 1, 'STARTED': 2, 'RELOCATED': 3, 'CLOSED': 4}
HOST_IDX = -1
LOG_FILENAME = 'es2graphite.log'
LOG_PATH = '.'
loglevel = {    'info': logging.INFO,
                'warn': logging.WARN,
                'error': logging.ERROR,
                'debug': logging.DEBUG
            }

root_logger = logging.getLogger()
file_handler = logging.handlers.RotatingFileHandler("{0}/{1}".format(LOG_PATH, LOG_FILENAME), 
                                                    maxBytes=100000000, 
                                                    backupCount=5)
root_logger.addHandler(file_handler)


def log(what, force=False):
    logging.info(what)

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
        
def process_node_stats(prefix, stats):
    metrics = []
    global CLUSTER_NAME
    CLUSTER_NAME = stats['cluster_name']
    for node_id in stats['nodes']:
        node_stats = stats['nodes'][node_id]
        NODES[node_id] = node_stats['name']
        process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, NODES[node_id]), node_stats)
    return metrics

def process_cluster_health(prefix, health):
    metrics = []
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME), health)
    return metrics

def process_indices_status(prefix, status):
    metrics = []
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), status['indices'])
    return metrics
    
def process_indices_stats(prefix, stats):
    metrics = []
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices', '_all'), stats['_all'])
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), stats['indices'])
    return metrics
    
def process_segments_status(prefix, status):
    metrics = []
    process_section(int(time.time()), metrics, (prefix, CLUSTER_NAME, 'indices'), status['indices'])
    return metrics
    
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

def submit_to_graphite(metrics):
    graphite_socket = {'socket': socket.socket( socket.AF_INET, socket.SOCK_STREAM ), 
                       'host': args.graphite_host, 
                       'port': int(args.graphite_port)}

    try:
        graphite_socket['socket'].connect( ( graphite_socket['host'], int( graphite_socket['port'] ) ) )
    except socket.error, serr:
        logging.error('Connection to Graphite server failed: ' + str(serr))

    for metric_name, metric_list in metrics:
        metric_string = "%s %s %d" % ( metric_name, metric_list[1], metric_list[0])
        logging.debug('Metric String: ' + metric_string)
        if args.dry_run:
            pass
        else:
            try:
                graphite_socket['socket'].send( "%s\n" % metric_string )
            except socket.error, serr:
                logging.debug('Connection to Graphite server failed: ' + str(serr))

 
def get_metrics():
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    node_stats_url = 'http://%s/_nodes?all=true' % get_es_host()
    log('%s: GET %s' % (dt, node_stats_url))
    node_stats_data = urllib2.urlopen(node_stats_url).read()
    node_stats = json.loads(node_stats_data)
    node_stats_metrics = process_node_stats(args.prefix, node_stats)
    submit_to_graphite(node_stats_metrics)
 
    cluster_health_url = 'http://%s/_cluster/health?level=%s' % (get_es_host(), args.health_level)
    log('%s: GET %s' % (dt, cluster_health_url))
    cluster_health_data = urllib2.urlopen(cluster_health_url).read()
    cluster_health = json.loads(cluster_health_data)
    cluster_health_metrics = process_cluster_health(args.prefix, cluster_health)
    submit_to_graphite(cluster_health_metrics)

    indices_stats_url = 'http://%s/_stats?all=true' % get_es_host()
    if args.shard_stats:
        indices_stats_url = '%s&level=shards' % indices_stats_url
    log('%s: GET %s' % (dt, indices_stats_url))
    indices_stats_data = urllib2.urlopen(indices_stats_url).read()
    indices_stats = json.loads(indices_stats_data)
    indices_stats_metrics = process_indices_stats(args.prefix, indices_stats)
    submit_to_graphite(indices_stats_metrics)
   
    if args.segments:
        segments_status_url = 'http://%s/_segments' % get_es_host()
        log('%s: GET %s' % (dt, segments_status_url))
        segments_status_data = urllib2.urlopen(segments_status_url).read()
        segments_status = json.loads(segments_status_data)
        segments_status_metrics = process_segments_status(args.prefix, segments_status)
        submit_to_graphite(segments_status_metrics)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send elasticsearch metrics to graphite')
    parser.add_argument('-p', '--prefix', default='es', help='graphite metric prefix. Default: %(default)s')
    parser.add_argument('-g', '--graphite-host', default='localhost', help='graphite hostname. Default: %(default)s')
    parser.add_argument('-o', '--graphite-port', default=2004, type=int, help='graphite pickle protocol port. Default: %(default)s')
    parser.add_argument('-i', '--interval', default=60, type=int, help='interval in seconds. Default: %(default)s')
    parser.add_argument('--health-level', choices=['cluster', 'indices', 'shards'], default='indices', help='The level of health metrics. Default: %(default)s')
    parser.add_argument('--log-level', choices=['info', 'warn', 'error', 'debug'], default='warn', help='The logging level. Default: %(default)s')
    parser.add_argument('--stdout', action='store_true', help='output logging to stdout. Default: %(default)s')
    parser.add_argument('--shard-stats', action='store_true', help='Collect shard level stats metrics. Default: %(default)s')
    parser.add_argument('--segments', action='store_true', help='Collect low-level segment metrics. Default: %(default)s')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Print metrics, don\'t send to graphite. Default: %(default)s')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output. Default: %(default)s')
    parser.add_argument('es', nargs='+', help='elasticsearch host:port', metavar='ES_HOST')
    args = parser.parse_args()


    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-5.12s] [%(levelname)-8.8s]  %(message)s")
    if args.log_level.lower() == 'debug':
        logFormatter = logging.Formatter("%(asctime)s [%(threadName)-5.12s %(filename)-20.20s:%(funcName)-5.5s:%(lineno)-3d] [%(levelname)-8.8s]  %(message)s")
    if args.stdout:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logFormatter)
        root_logger.addHandler(stream_handler)

    file_handler.setFormatter(logFormatter)
    root_logger.setLevel(loglevel[args.log_level])

    while True:
        if args.dry_run:
            logging.warn('Metric not Submitted. Processing as a Dry Run.')
        get_metrics()
        time.sleep(args.interval)
