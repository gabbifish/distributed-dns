# Distributed DNS Server Cluster
This implementation of a distributed DNS server cluster allows multiple nodes
to respond to DNS queries, while maintaining consistent resource records among
them and propogating the addition of new resource records to one node.

Flotsam is a course project for the autumn 2017 offering of Stanford
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

## Required Setup
The nodes in this distributed DNS cluster rely on the Twisted and pySyncObj
python libaries. Pip install both before attempting to run.

### Run
The DNS server relies on the dns-config file, which is parsed as json. This
file specifies which nodes use which ip:port pairs for querying and
participating in raft.

To run a node, run
```
./dns-node -n X -c dns-config.json -z zfX.txt
```
where X is the number of the node (and its corresponding zonefile) you want to
start. Because raft will not work on a single node, a running node will hang at
`Initializing raft...` until another node is started up.

## Extensibility
TODO
