#!/usr/bin/env python
import argparse
import base64
from StringIO import StringIO
from hashlib import sha256
import json
from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplDict, ReplLockManager
import sys
import thread
import threading
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

        self.__rr_local = {}
        self.__node, self.__other_nodes, self.__query_port, self.__num_nodes = self.__configure()

        self.__lock = threading.RLock()

        self.__rr_raft, self.__rr_dwnlds = self.__initDistributedDict()
        self.__loadZones()

        self.__thread = threading.Thread(target=lambda: self.readFromStdin())
        self.__thread.setDaemon(True)
        self.__thread.start()


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

    def readFromStdin(self):
        while True:
            line = ""
            try:
                line = sys.stdin.readline()
                rr = self.__parseLine(line)
            except Exception:
                print("Some error prevented parsing line: %s" %line)
                continue
            print "Read line"
            prefixKey = self._getPrefixKey(rr.name.name, rr.type)
            fullKey = self.__getUniqueKey(rr)
            self.__addLocalStorage(rr, prefixKey, fullKey)
            self.__addRaftStorage(rr, prefixKey, fullKey)
            print "Added line"


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
        return repr((rrheader.name.name,
                    rrheader.type,
                    base64.b64encode(hashval)))

    def _getPrefixKey(self, name, qtype):
        return repr((name,qtype))

    def __parseLine(self, line):
        tokens = line.split(None, 2)
        # reject if incorrectly formatted.
        if len(tokens) != 3:
            raise RuntimeError(
                "line '%s': wrong # of tokens %d." %(line, len(tokens)))

        rname, rtype, rvalue = tokens
        # # if rvalue is a list, make sure to store it as one!
        if rvalue.startswith("["):
            rvalue = json.loads(rvalue)

        # create correct payload
        payload = None
        if rtype == "A":
            payload = dns.Record_A(address=rvalue)
        elif rtype == "CNAME":
            payload = dns.Record_CNAME(name=rvalue)
        elif rtype == "MX":
            payload = dns.Record_MX(name=rvalue[0], 
                                    preference=int(rvalue[1]))
        elif rtype == "NS":
            payload = dns.Record_NS(name=rvalue)
        elif rtype == "SOA":
            payload = dns.Record_SOA(mname=rvalue[0], rname=rvalue[1])
        elif rtype == "TXT":
            payload = dns.Record_TXT(data=[rvalue])
        else:
            raise "cannot parse line!"
        
        return dns.RRHeader(name=rname, 
                            type=self.__query_types[rtype], 
                            payload=payload)
        
    def __addLocalStorage(self, rr, prefixKey, fullKey):
        # Add to local RR store
        with self.__lock:
            if self.__rr_local.get(prefixKey) is None:
                self.__rr_local[prefixKey] = [(rr, fullKey)]
            else:
                self.__rr_local[prefixKey].append((rr, fullKey))

    def __addRaftStorage(self, rr, prefix_key, full_key):
        #if self.__lock.tryAcquire('lock', sync=True):
        with self.__lock:
            if self.__rr_raft.get(prefix_key) is None:
                self.__rr_raft.set(prefix_key, [(rr, full_key)], sync=True)
            else:
                rr_list = self.__rr_raft.get(prefix_key)
                rr_list.append((rr, full_key))
                self.__rr_raft.set(prefix_key, rr_list, sync=True)
    
            if full_key not in self.__rr_dwnlds.keys():
                # You know one download of this value has already occured!
                self.__rr_dwnlds.set(full_key, 1, sync=True)
            else:
                prev_value = self.__rr_dwnlds[full_key]
                self.__rr_dwnlds.set(full_key, prev_value+1, sync=True)
                #self.__lock.release('lock')

    # Read in entries from zone file and store locally.
    def __loadZones(self):
        print "Loading zonefile..."
        records = {}
        for line in self.__zoneLines():
            
            rr = None
            try:
                rr = self.__parseLine(line)
            except Exception:
                print("Some error prevented parsing line: %s" %line)
            print str(rr)#, str(rr.payload)
            full_key = self.__getUniqueKey(rr)
            prefix_key = self._getPrefixKey(rr.name.name, rr.type)
            
            self.__addLocalStorage(rr, prefix_key, full_key)
            self.__addRaftStorage(rr, prefix_key, full_key)
            
        return records

    '''
    PRIVATE FUNC
    func _initDistributedDict() sets up distributed dict for requests not yet
    written locally.
    '''
    def __initDistributedDict(self):
        rr_raft = ReplDict()
        rr_dwnlds = ReplDict()
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.__node, self.__other_nodes,
        consumers=[rr_raft, rr_dwnlds], conf=config)

        print "Initializing Raft..."
        while not syncObj.isReady():
            continue

        print "Raft initialized!"
        return rr_raft, rr_dwnlds

    '''
    PRIVATE FUNC
    func _recordLookup() looks up RRs that match a query.
    '''
    def __recordLookup(self, query):
        qname = query.name.name
        qtype = query.type
        print time.time()

        answer_dict = {}
        authority = []
        additional = []

        for k, v in self.__rr_local.iteritems():
            sys.stdout.write("k: %s\n" %k)
            # for p in v:
            #     sys.stdout.write("payload:%s hash:%s\n" %(str(p[0].payload), str(p[1])))
            # sys.stdout.flush()

        #print "RAFT:"
        #for k, v in self.__rr_raft.items():
        #   sys.stdout.write("k: %s\n" %k)
           # for p in v:
           #     sys.stdout.write("payload:%s hash:%s\n" %(str(p[0].payload), str(p[1])))
           # sys.stdout.flush()
        

        prefix_key = self._getPrefixKey(qname, qtype)
        local_matches = None
        with self.__lock:
            local_matches = self.__rr_local.get(prefix_key)
        if local_matches:
            for (new_rr, full_key) in local_matches:
                answer_dict[full_key] = new_rr

        # Next, attempt to get it from distributed_dict
        # of full_keys->RR. Need to do quick lookup to see if any keys of
        # qname-qtype* exist; these should be added to our answers list if these
        # records aren't already there.
        raft_matches = None
        with self.__lock:
            raft_matches = self.__rr_raft.get(prefix_key)

        if raft_matches:
            raft_matches_cpy = list(raft_matches)# TODO: use defer

            for (new_rr, full_key) in raft_matches:
                if answer_dict.get(full_key) is not None:
                    continue

                # if record is not a duplicate, add it to answer_dict.
                answer_dict[full_key] = new_rr

                # Also insert local copy!
                if self.__rr_local.get(prefix_key) is None:
                    self.__rr_local[prefix_key] = [(new_rr, full_key)]
                self.__rr_local[prefix_key].append((new_rr, full_key))

                #if self.__lock.tryAcquire('lock', sync=True):
                prev_value = self.__rr_dwnlds[full_key]
                if prev_value+1 >= self.__num_nodes:
                    self.__rr_dwnlds.pop(full_key, sync=True)
                    raft_matches_cpy.remove((new_rr, full_key))
                else:
                    self.__rr_dwnlds.set(full_key, prev_value+1, sync=True)

                #    self.__lock.release('lock')

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
