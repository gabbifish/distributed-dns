import argparse
import base64
from cStringIO import StringIO
from hashlib import sha256
import json
from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplDict, ReplLockManager
import time
from twisted.internet import reactor, defer
from twisted.names import client, dns, error, server

class Resolver:

    '''
    Initialize resolver
    '''
    def __init__(self, node_name, zone_file, config_file):
        self.node_name = node_name
        self.zone_file = zone_file
        self.config_file = config_file
        self.query_types = {
            "A" : dns.A,
            "CNAME" : dns.CNAME,
            "MX" : dns.MX,
            "NS" : dns.NS,
            "SOA" : dns.SOA,
            "TXT" : dns.TXT
        }

        self.node, self.other_nodes, self.query_port, self.num_nodes = self._configure()

        self.rr_raft, self.rr_dwnlds, self.lock = self._initDistributedDict()
        self.rr_local = self._loadZones()

    '''
    PRIVATE FUNC
    func _configure() is called upon resolver initialization; returns self_node
    ip:port and list of other ip:ports for other nodes in cluster.
    '''
    def _configure(self):
        config_fd = open(self.config_file)
        configs = json.load(config_fd)

        # Get node specified on commandline.
        node = configs["nodes"][self.node_name]["raft_loc"]

        # Get list of other nodes in cluster.
        other_nodes = []
        for k in configs["nodes"].keys():
            if k != self.node_name:
                other_nodes.append(configs["nodes"][k]["raft_loc"])

        # Get query port
        query_loc = configs["nodes"][self.node_name]["query_loc"]
        query_port = int(query_loc.split(":")[1])

        # Get total number of nodes
        num_nodes = len(configs["nodes"])

        config_fd.close()

        return node, other_nodes, query_port, num_nodes

    '''
    PUBLIC FUNC
    func getQueryPort() returns this node's query port--the one used by
    command-line tools like dig (e.g. 'dig -p 10053 @127.0.0.1 example.com' if
    query_port is 10053 in config file)
    '''
    def getQueryPort(self):
        return self.query_port

    '''
    PRIVATE FUNC
    func _zoneLines() processes lines in the zonefile, ignoring commented ones
    and yielding entries stripped of external whitespace.
    '''
    def _zoneLines(self):
        zone_fd = open(self.zone_file)
        for line in zone_fd:
            if line.startswith("#"):
                continue
            line = line.rstrip()
            yield line

    def _getUniqueKey(self, rrheader):
        encoder = StringIO()
        m = sha256()
        m.update(str(rrheader.payload))
        hashval = m.digest()
        return repr((str(rrheader.name),
                    rrheader.type,
                    base64.b64encode(hashval)))

    def _getprefix_key(self, name, qtype):
        rdata = (name, qtype)
        m = sha256()
        m.update(repr(rdata))
        hashval = m.digest()
        return repr((str(name),
                    qtype))

    # Read in entries from zone file and store locally.
    def _loadZones(self):
        print "Loading zonefile..."
        records = {}
        for line in self._zoneLines():
        #try:
            line_components = line.split(None, 2)
            # Case if independent record
            if len(line_components) == 3:
                rname, rtype, rvalue = line_components
                # # if rvalue is a list, make sure to store it as one!
                payload = None
                if rvalue.startswith("["):
                    rvalue = json.loads(rvalue)
                # create correct payload
                payload = None
                if rtype == "A":
                    payload = dns.Record_A(address=rvalue)
                elif rtype == "CNAME":
                    payload = dns.Record_CNAME(name=rvalue)
                elif rtype == "MX":
                    payload = dns.Record_MX(name=rvalue[0], preference=int(rvalue[1]))
                elif rtype == "NS":
                    payload = dns.Record_NS(name=rvalue)
                elif rtype == "SOA":
                    payload = dns.Record_SOA(mname=rvalue[0], rname=rvalue[1])
                elif rtype == "TXT":
                    payload = dns.Record_TXT(data=[rvalue])
                    # UGHGHHG still have to figure out how to
                    # handle multiple line TXT inputs.
                new_rr = dns.RRHeader(name=rname, type=self.query_types[rtype], payload=payload)
                full_key = self._getUniqueKey(new_rr)
                prefix_key = self._getprefix_key(new_rr.name.name, new_rr.type)

                # print "new_rr is %s%s" % (str(new_rr), str(new_rr.payload))
                # print "full key is %s" % str(full_key)
                # print "prefix key is %s" % str(prefix_key)

                # Add to local RR store
                if records.get(prefix_key) is None:
                    records[prefix_key] = []
                records[prefix_key].append((new_rr, full_key))

                # Add to raft RR store
                # print "Record %s is being added to the raft RR store" % str(new_rr)
                # if self.lock.tryAcquire('lock', sync=True):
                # print "adding to raft new rr " + str(new_rr)
                if self.rr_raft.get(prefix_key) is None:
                    self.rr_raft.set(prefix_key, [], sync=True)
                rr_list = self.rr_raft.get(prefix_key)
                rr_list.append((new_rr, full_key))
                self.rr_raft.set(prefix_key, rr_list, sync=True)
                if full_key not in self.rr_dwnlds.keys():
                    # You know one download of this value has already occured!
                    self.rr_dwnlds.set(full_key, 1, sync=True)
                else:
                    prev_value = self.rr_dwnlds[full_key]
                    self.rr_dwnlds.set(full_key, prev_value+1, sync=True)
                # self.lock.release('lock')
            else:
                raise RuntimeError("line '%s' has an incorrect number of args %d." % line, len(line_components))
            #except Exception as e:
            #    raise RuntimeError("line '%s' is incorrectly formatted." % line)
        # print "AFTER ADDING ALL KEYS TO RAFT LIST, IT LOOKS LIKE"
        # print self.rr_raft.keys()
        return records

    '''
    PRIVATE FUNC
    func _initDistributedDict() sets up distributed dict for requests not yet
    written locally.
    '''
    def _initDistributedDict(self):
        rr_raft = ReplDict()
        rr_dwnlds = ReplDict()
        lock = ReplLockManager(3)
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.node, self.other_nodes,
        consumers=[rr_raft, rr_dwnlds, lock], conf=config)

        print "Initializing Raft..."
        while not syncObj.isReady():
            continue

        # now distributed_dict is ready!
        print "Raft initialized!"
        return rr_raft, rr_dwnlds, lock

    '''
    PRIVATE FUNC
    func _recordLookup() looks up RRs that match a query.
    '''
    def _recordLookup(self, query):
        qname = query.name.name
        qtype = query.type

        answer_dict = {}
        authority = []
        additional = []

        prefix_key = self._getprefix_key(qname, qtype)
        # print "Current prefix key is " + str(prefix_key)
        local_matches = self.rr_local.get(prefix_key)
        if local_matches:
            # print "local_matches is %s" % str(local_matches)
            # print "Found local match"
            for (new_rr, full_key) in local_matches:
                answer_dict[full_key] = new_rr

        # Next, attempt to get it from distributed_dict
        # of full_keys->RR. Need to do quick lookup to see if any keys of
        # qname-qtype* exist; these should be added to our answers list if these
        # records aren't already there.
        raft_matches = self.rr_raft.get(prefix_key)
        raft_matches_cpy = list(raft_matches)# TODO: use defer
        # print "Raft keys and value lists are currently " + str([(key, len(self.rr_raft[key])) for key in self.rr_raft.keys()])
        if raft_matches:
            # print "old raft table for this prefix key is " + str(self.rr_raft[prefix_key])

            # print "raft_matches is %s" % str(raft_matches)
            for (new_rr, full_key) in raft_matches:
                if full_key in answer_dict.keys():
                    # print "Already exists!"
                    continue

                # if record is not a duplicate, add it to answer_dict.
                # print "New record from raft! Adding it."
                answer_dict[full_key] = new_rr

                # Also insert local copy!
                if self.rr_local.get(prefix_key) is None:
                    self.rr_local[prefix_key] = []
                self.rr_local[prefix_key].append((new_rr, full_key))

                if self.lock.tryAcquire('lock', sync=True):
                    # print "rr_dwnlds.keys() is " + str(self.rr_dwnlds.keys())
                    prev_value = self.rr_dwnlds[full_key]
                    if prev_value+1 >= self.num_nodes:
                        # print "removal occurred!"
                        self.rr_dwnlds.pop(full_key, sync=True)
                        # remove rr from rr_raft list
                        # rr_raft.pop(full_key)
                        raft_matches_cpy.remove((new_rr, full_key))
                    else:
                        self.rr_dwnlds.set(full_key, prev_value+1, sync=True)

                    self.lock.release('lock')

        self.rr_raft.set(prefix_key, raft_matches_cpy, sync=True)
        # print "new raft table for this prefix key is " + str(self.rr_raft[prefix_key])

        if len(answer_dict) > 0: # corresponding RRs have been found
            return answer_dict.values(), authority, additional
        # Copy RR locally before returning, and also increment the copy count in the hashes->local_copy_count. USE LOCKS HERE.

        # TODO if no records, return NX record. placeholder is empty set of
        # answers for now.
        answer = []
        return answer, authority, additional

    '''
    PUBLIC FUNC
    func query() returns RRs that match a query.
    '''
    def query(self, query, timeout=None):
        return defer.succeed(self._recordLookup(query))

    '''
    TODO: called from separate thread; if new mapping is entered via
    commandline, ensure that it is added to the distributed_dict!
    '''
    # def add_entry(self):


if __name__ =='__main__':
    # Parse args; node name from dns-config and zonefile are required.
    parser = argparse.ArgumentParser(description='Start up node in DNS cluster')
    parser.add_argument('-n', '--node', required=True,
                        help='this node\'s id')
    parser.add_argument('-z', '--zone', required=True,
                        help='this node\'s zonefile')
    parser.add_argument('-c', '--config', required=True,
                        help='this DNS server cluster\'s config file')
    args = parser.parse_args()
    node_name = args.node
    zone_file = args.zone
    config_file = args.config

    # Start resolver
    resolver = Resolver(node_name, zone_file, config_file)
    # get port for resolver
    port = resolver.getQueryPort()

    factory = server.DNSServerFactory(
        clients=[resolver, client.Resolver(resolv='/etc/resolv.conf')]
    )
    protocol = dns.DNSDatagramProtocol(controller=factory)

    # Run the server on provided port.
    reactor.listenUDP(port, protocol)
    reactor.listenTCP(port, factory)

    print "Starting DNS server on port %d..." % port
    reactor.run()
