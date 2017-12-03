from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict
import time
import argparse
import json

class Resolver:
    # Initialize resolver
    def __init__(self, node_name, zone_file, config_file):
        self.node_name = node_name
        self.zone_file = zone_file
        self.config_file = config_file

        self.node, self.other_nodes = self.configure()

        self.records = self.init_records()
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

    # TODO: Read in entries from zone file and store locally.
    def init_records(self):
        # First, load in this server's zone file.
        zone_fd = open(self.zone_file)
        return zone_fd
        # TODO: FINISH

    # Next, set up distributed dict for requests not yet written locally.
    def init_distributed_dict(self):
        distributed_dict = ReplDict()
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.node, self.other_nodes, consumers=[distributed_dict], conf=config)

        while not syncObj.isReady():
            continue
        # now distributed_dict is ready!
        return distributed_dict

    # TODO: DNS record lookup (if in zone list) OR request info for given
    # domain!
    def resolve():
        return

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
