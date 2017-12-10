import argparse
import base64
from StringIO import StringIO
from hashlib import sha256
import json
from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplDict, ReplLockManager
import thread
import time
from twisted.internet import reactor, defer
from twisted.names import client, dns, error, server

class Resolver:

    '''
    Initialize resolver
    '''
    def __init__(self, node_name, zone_file, config_file):
        self.__node_name = node_name
        self.__zone_file = zone_file
        self.__config_file = config_file
        self.__query_types = {
            "A" : dns.A,
            "CNAME" : dns.CNAME,
            "MX" : dns.MX,
            "NS" : dns.NS,
            "SOA" : dns.SOA,
            "TXT" : dns.TXT
        }

        self.__node, self.__other_nodes, self.__query_port, self.__num_nodes = self.__configure()

        self.__rr_raft, self.__rr_dwnlds, self.__lock = self.__initDistributedDict()
        self.__rr_local = self.__loadZones()

    '''
    PRIVATE FUNC
    func _configure() is called upon resolver initialization; returns self_node
    ip:port and list of other ip:ports for other nodes in cluster.
    '''
    def __configure(self):
        config_fd = open(self.__config_file)
        configs = json.load(config_fd)

        # Get node specified on commandline.
        node = configs["nodes"][self.__node_name]["raft_loc"]

        # Get list of other nodes in cluster.
        other_nodes = []
        for k in configs["nodes"].keys():
            if k != self.__node_name:
                other_nodes.append(configs["nodes"][k]["raft_loc"])

        # Get query port
        query_loc = configs["nodes"][self.__node_name]["query_loc"]
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
        return self.__query_port

    '''
    PRIVATE FUNC
    func _zoneLines() processes lines in the zonefile, ignoring commented ones
    and yielding entries stripped of external whitespace.
    '''
    def __zoneLines(self):
        zone_fd = open(self.__zone_file)
        for line in zone_fd:
            if line.startswith("#"):
                continue
            line = line.rstrip()
            yield line

    def __getUniqueKey(self, rrheader):
        encoder = StringIO()
        m = sha256()
        m.update(str(rrheader.payload))
        hashval = m.digest()
        return repr((str(rrheader.name),
                    rrheader.type,
                    base64.b64encode(hashval)))

    def _getPrefixKey(self, name, qtype):
        return repr((name,qtype))

    # Read in entries from zone file and store locally.
    def __loadZones(self):
        print "Loading zonefile..."
        records = {}
        for line in self.__zoneLines():
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
                new_rr = dns.RRHeader(name=rname, type=self.__query_types[rtype], payload=payload)
                full_key = self.__getUniqueKey(new_rr)
                prefix_key = self.__getPrefixKey(new_rr.name.name, new_rr.type)

                # Add to local RR store
                if records.get(prefix_key) is None:
                    records[prefix_key] = []
                records[prefix_key].append((new_rr, full_key))

                if self.__lock.tryAcquire('lock', sync=True):
                    if self.__rr_raft.get(prefix_key) is None:
                        self.__rr_raft.set(prefix_key, [], sync=True)
                    rr_list = self.__rr_raft.get(prefix_key)
                    rr_list.append((new_rr, full_key))
                    self.__rr_raft.set(prefix_key, rr_list, sync=True)
                    if full_key not in self.__rr_dwnlds.keys():
                        # You know one download of this value has already occured!
                        self.__rr_dwnlds.set(full_key, 1, sync=True)
                    else:
                        prev_value = self.__rr_dwnlds[full_key]
                        self.__rr_dwnlds.set(full_key, prev_value+1, sync=True)
                    self.__lock.release('lock')
            else:
                raise RuntimeError("line '%s' has an incorrect number of args %d." % line, len(line_components))
        return records

    '''
    PRIVATE FUNC
    func _initDistributedDict() sets up distributed dict for requests not yet
    written locally.
    '''
    def __initDistributedDict(self):
        rr_raft = ReplDict()
        rr_dwnlds = ReplDict()
        lock = ReplLockManager(3)
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.__node, self.__other_nodes,
        consumers=[rr_raft, rr_dwnlds, lock], conf=config)

        print "Initializing Raft..."
        while not syncObj.isReady():
            continue

        print "Raft initialized!"
        return rr_raft, rr_dwnlds, lock

    '''
    PRIVATE FUNC
    func _recordLookup() looks up RRs that match a query.
    '''
    def __recordLookup(self, query):
        qname = query.name.name
        qtype = query.type

        answer_dict = {}
        authority = []
        additional = []

        prefix_key = self.__getPrefixKey(qname, qtype)
        local_matches = self.__rr_local.get(prefix_key)
        if local_matches:
            for (new_rr, full_key) in local_matches:
                answer_dict[full_key] = new_rr

        # Next, attempt to get it from distributed_dict
        # of full_keys->RR. Need to do quick lookup to see if any keys of
        # qname-qtype* exist; these should be added to our answers list if these
        # records aren't already there.
        raft_matches = self.__rr_raft.get(prefix_key)
        if raft_matches:
            raft_matches_cpy = list(raft_matches)# TODO: use defer

            for (new_rr, full_key) in raft_matches:
                if full_key in answer_dict.keys():
                    continue

                # if record is not a duplicate, add it to answer_dict.
                answer_dict[full_key] = new_rr

                # Also insert local copy!
                if self.__rr_local.get(prefix_key) is None:
                    self.__rr_local[prefix_key] = []
                self.__rr_local[prefix_key].append((new_rr, full_key))

                if self.__lock.tryAcquire('lock', sync=True):
                    prev_value = self.__rr_dwnlds[full_key]
                    if prev_value+1 >= self.__num_nodes:
                        self.__rr_dwnlds.pop(full_key, sync=True)
                        raft_matches_cpy.remove((new_rr, full_key))
                    else:
                        self.__rr_dwnlds.set(full_key, prev_value+1, sync=True)

                    self.__lock.release('lock')

            # remove rrs downloaded among all nodes from rr_raft list
            self.__rr_raft.set(prefix_key, raft_matches_cpy, sync=True)

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
        return defer.succeed(self.__recordLookup(query))

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
