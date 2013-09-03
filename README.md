### es2graphite

es2graphite gathers elasticsearch stats and status information 
and sends them to a graphite server.  One or more elasticsearch
hosts can be specified and requests will be round-robined between
each server.  

### metrics

* node stats
* indices status
* indices segments status
* indices stats (including shards)
* cluster health (including shards)

### notes

* each metric starts with a prefix + the cluster name
* metrics will be sent to graphite at the specified interval
* node names will be used in metrics vs. node id's
* if a shard is moved to a new node, a new metric using the new node name is used
* cluster status: 
    * 0 = red
    * 1 = yellow
    * 2 = green
* shard state: 
    * 0 = created
    * 1 = recovering
    * 2 = started
    * 3 = relocated
    * 4 = closed
* the graphite pickle protocol is used, make sure it is enabled

```
usage: es2graphite.py [-h] [-p PREFIX] [-g GRAPHITE_HOST] [-o GRAPHITE_PORT]
                      [-i INTERVAL]
                      ES_HOST [ES_HOST ...]

Send elasticsearch metrics to graphite

positional arguments:
  ES_HOST               elasticsearch host:port

optional arguments:
  -h, --help            show this help message and exit
  -p PREFIX, --prefix PREFIX
                        graphite metric prefix. Default: es
  -g GRAPHITE_HOST, --graphite-host GRAPHITE_HOST
                        graphite hostname. Default: localhost
  -o GRAPHITE_PORT, --graphite-port GRAPHITE_PORT
                        graphite pickle protocol port. Default: 2004
  -i INTERVAL, --interval INTERVAL
                        interval in seconds. Default: 60
```
