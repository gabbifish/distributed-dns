from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict
import time
import argparse
import json
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

        self.node, self.other_nodes, self.query_port = self._configure()

        self.records = self._loadZones()
        self.distributed_dict = self._initDistributedDict()

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

        config_fd.close()

        return node, other_nodes, query_port

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

    # Read in entries from zone file and store locally.
    def _loadZones(self):
        records = []
        for line in self._zoneLines():
            try:
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
                    records.append(new_rr)
                # Case if line is continuation of other record (e.g. long TXT)
                # elif len(line_components) == 1:

                # Case neither of above--odd line
                else:
                    raise RuntimeError("line '%s' has an incorrect number of args %d." % line, len(line_components))
            except Exception as e:
                raise RuntimeError("line '%s' is incorrectly formatted." % line)

        return records

    '''
    PRIVATE FUNC
    func _initDistributedDict() sets up distributed dict for requests not yet
    written locally.
    '''
    def _initDistributedDict(self):
        distributed_dict = ReplDict()
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.node, self.other_nodes,
        consumers=[distributed_dict], conf=config)

        while not syncObj.isReady():
            continue

        # now distributed_dict is ready!
        print "Raft initialized..."
        return distributed_dict

    '''
    PRIVATE FUNC
    func _recordLookup() looks up RRs that match a query.
    '''
    def _recordLookup(self, query):
        name = query.name.name
        qtype = query.type
        # check if mapping in zonefile init_records
        answers = []
        authority = []
        additional = []
        for rr in self.records:
            if rr.name.name == name and rr.type == qtype:
                answers.append(rr)
        # TODO if not, return NX record (to maintain availability)

        return answers, authority, additional

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
