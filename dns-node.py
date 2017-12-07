from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict
import time
import argparse
import json
from twisted.names import client, dns, error, server

query_types = {
    "A" : dns.A,
    "CNAME" : dns.CNAME,
    "MX" : dns.MX,
    "NS" : dns.NS,
    "SOA" : dns.SOA,
    "TXT" : dns.TXT
}

class Resolver:

    # Initialize resolver
    def __init__(self, node_name, zone_file, config_file):
        self.node_name = node_name
        self.zone_file = zone_file
        self.config_file = config_file

        self.node, self.other_nodes = self.configure()

        self.records = self.load_zones()
        self.distributed_dict = self.init_distributed_dict()

    # Called upon resolver initialization; gets self_node ip:port and list of
    # other ip:ports for other nodes in cluster.
    def configure(self):
        configs = json.load(open(self.config_file))

        # Get node specified on commandline.
        node = configs["nodes"][self.node_name]

        # Get list of other nodes in cluster.
        other_nodes = []
        for k in configs["nodes"].keys():
            if k != self.node_name:
                other_nodes.append(configs["nodes"][k])

        return node, other_nodes

    def zone_lines(self):
        zone_fd = open(self.zone_file)
        for line in zone_fd:
            if line.startswith("#"):
                continue
            line = line.rstrip()
            yield line

    # Read in entries from zone file and store locally.
    def load_zones(self):
        records = []
        for line in self.zone_lines():
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
                        freeform = []
                        freeform.append(rvalue)
                        payload = dns.Record_TXT(data=freeform)
                        # UGHGHHG still have to figure out how to
                        # handle multiple line TXT inputs.

                    new_rr = dns.RRHeader(name=rname, type=query_types[rtype], payload=payload)
                    records.append(new_rr)
                # Case if line is continuation of other record (e.g. long TXT)
                # elif len(line_components) == 1:

                # Case neither of above--odd line
                else:
                    raise RuntimeError("line '%s' has an incorrect number of args %d." % line, len(line_components))
            except Exception as e:
                raise RuntimeError("line '%s' is incorrectly formatted." % line)

        for record in records:
            print record.payload

    # Next, set up distributed dict for requests not yet written locally.
    def init_distributed_dict(self):
        distributed_dict = ReplDict()
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.node, self.other_nodes, consumers=[distributed_dict], conf=config)

        while not syncObj.isReady():
            continue
        # now distributed_dict is ready!
        return distributed_dict

    # TODO: DNS record lookup (if in zone list) OR NX record
    # def query(self, query, timeout=None):
        # check if mapping in zonefile init_records

        # if not, return NX record (to maintain availability)

    # called from separate thread; if new mapping is entered via commandline,
    # ensure that it is added to the distributed_dict!
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
