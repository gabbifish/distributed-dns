# Distributed DNS Server Cluster
This implementation of a distributed DNS server cluster allows multiple nodes
(nameservers) to respond to direct DNS queries, while maintaining consistent
resource records among them and propogating the addition of new resource
records to one node.

This is a course project for the autumn 2017 offering of Stanford
[CS244B](http://cs244b.scs.stanford.edu).

## Node design
Our DNS server achieves high availability by maintaining a local datastore of
resource records, which periodically copies new resource records from the
raft-backed datastore. When an entry in the raft-backed datastore has been
copied into the local datastores of all nodes, it is removed from the
raft-backed datastore. This "checkpointing" process ensures that resource
records are saved locally so that every DNS server can answer DNS queries even
if the raft datastore becomes unavailable (e.g. when a majority of the nodes in
the DNS cluster fail).

Each node functions as both a nameserver and a simple resolver over that
nameserver's records.

## Required Setup
The nodes in this distributed DNS cluster rely on the Twisted and pySyncObj
python libaries. Pip install both before attempting to run.

## Run
The DNS server relies on the dns-config file, which is parsed as json. This
file specifies which nodes use which ip:port pairs for querying and
participating in raft.

### Starting DNS nameserver nodes

To run a node, run
```
./dns-node -n X -c dns-config.json -z zone-files/zfX.txt
```
where X is the number of the node (and its corresponding zonefile) you want to
start. Because raft will not work on a single node, a running node will hang at
`Initializing raft...` until another node is started up.

To test the DNS server as it runs locally, you can run a query over dig using
the node's query port.
```
dig -p 10053 @localhost example.com MX
```

To test the DNS server's efficiency, run the metrics-collection.py script in the
metrics directory.

### Adding and Removing DNS nodes at runtime
To add a resource record to a node, enter the following in a node proc's STDIN, where the whitespace between the fields are spaces or tabs. The first field is the action performed (ADD: or REMOVE:) and the following fields are resource name, resource type, and resource value/payload.
```
ADD:  newdomain.com  A  1.3.3.7  
```

Conversely, to remove resource records, enter the following:
```
REMOVE:  newdomain.com  A  1.3.3.7  
```
