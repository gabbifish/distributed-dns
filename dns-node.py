#!/usr/bin/env python
import argparse
import base64
from collections import deque
from StringIO import StringIO
from hashlib import sha256
import json
from pysyncobj import replicated
from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplDict, ReplLockManager
import sys
import thread
import threading
from twisted.internet import reactor, defer
from twisted.names import client, dns, error, server

num_responses = 0

class Resolver:

    '''
    Initialize resolver
    '''
    def __init__(self, node_name, zone_file, config_file, silent):
        self.__node_name = node_name
        self.__zone_file = zone_file
        self.__config_file = config_file
        self.__silent = silent
        self.__query_types = {
            "A" : dns.A,
            "CNAME" : dns.CNAME,
            "MX" : dns.MX,
            "NS" : dns.NS,
            "SOA" : dns.SOA,
            "TXT" : dns.TXT
        }

        self.__node, self.__other_nodes, self.__query_port, self.__num_nodes = self.__configure()

        self.__lock = threading.RLock()

        self.__rr_local = self.__initDistributedDict()
        self.__loadZones()
        self.__count = 0
        self.__stdin_thread = threading.Thread(target=lambda: self.readFromStdin())
        self.__stdin_thread.setDaemon(True)
        self.__stdin_thread.start()


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
    func getQueryPort() returns this node's query port--the one used by
    command-line tools like dig (e.g. 'dig -p 10053 @127.0.0.1 example.com' if
    query_port is 10053 in config file)
    '''
    def getQueryPort(self):
        return self.__query_port

    '''
    func __initDistributedDict() sets up distributed dict for all resource
    records among the nodes
    '''
    def __initDistributedDict(self):
        rr_raft = ReplDict()
        config = SyncObjConf(appendEntriesUseBatch=True)
        syncObj = SyncObj(self.__node, self.__other_nodes,
        consumers=[rr_raft], conf=config)

        if not self.__silent:
            print "Initializing Raft..."
        while not syncObj.isReady():
            continue

        if not self.__silent:
            print "Raft initialized!"
        return rr_raft

    '''
    func readFromStdin() reads new resource records entered through a nodes'
    STDIN. It's used to demonstrate the propogation of new resource records
    among nodes in the DNS cluster. This function runs in its own thread to
    allow for queries while new records are written.
    '''
    def readFromStdin(self):
        while True:
            line = ""
            try:
                line = sys.stdin.readline()
                action, entry = self.__determineAction(line)
                rr = self.__parseLine(entry)
            except Exception:
                print "Some error prevented parsing line: %s" % line
                continue
            if not self.__silent:
                print "Read line into resource record %s." % str(rr)
            prefixKey = self._getPrefixKey(rr.name.name, rr.type)
            if action == "ADD:":
                self.__addLocalStorage(rr, prefixKey)
                if not self.__silent:
                    print "Added resource record for this entry."
            elif action == "REMOVE:":
                self.__removeLocalStorage(rr, prefixKey)
                if not self.__silent:
                    print "Removed resource record for this entry."


    '''
    func __determineAction() parses a STDIN line for whether it is a RR addition
    or removal.
    '''
    def __determineAction(self, line):
        action, rr_entry = line.split(None, 1)
        if action != "ADD:" and action != "REMOVE:":

            raise Exception("Incorrectly formatted entry or removal of resource record in line %s" % line)
        return action, rr_entry

    '''
    func __getUniqueKey() generates a unique key for a resource record by
    concatenating a RR's name, RR's type, and hash of the RR's payload. This is
    used to uniquely identify a RR.
    '''
    def __getUniqueKey(self, rrheader):
        encoder = StringIO()
        m = sha256()
        m.update(str(rrheader.payload))
        hashval = m.digest()
        return repr((rrheader.name.name,
                    rrheader.type,
                    base64.b64encode(hashval)))

    '''
    func __getPrefixKey() generates a key for all resource records of a unique
    RR name and RR type combination. This is used for performing queries over
    a set of RRs with the same name and type.
    '''
    def _getPrefixKey(self, name, qtype):
        return repr((name,qtype))

    '''
    func __addLocalStorage() adds resource records to the raft data store.
    The raft data store is a map of prefix keys to resource records.
    '''
    def __addLocalStorage(self, rr, prefixKey):
        # Add to RR store
        with self.__lock:
            if self.__rr_local.get(prefixKey) is None:
                self.__rr_local.set(prefixKey, [], sync=True)
            rr_list = self.__rr_local.get(prefixKey)
            if rr in rr_list:
                return
            else:
                rr_list.append(rr)
                self.__rr_local.set(prefixKey, rr_list)

    '''
    func __removeLocalStorage() removes resource records to the raft data store.
    The raft data store is a map of prefix keys to a list of tuples (resource
    record, unique hash of rr)
    '''
    def __removeLocalStorage(self, rr, prefixKey):
        # Remove from RR store
        with self.__lock:
            if self.__rr_local.get(prefixKey) is None:
                return
            rr_list = self.__rr_local.get(prefixKey)
            rr_list.remove(rr)
            if len(rr_list) > 0:
                self.__rr_local.set(prefixKey, rr_list)
            else:
                self.__rr_local.pop(prefixKey)

    '''
    func __loadZones() iterates over entries in the zonefile and creates
    Record_X objects for resource record type X. It adds these entries to the
    raft RR datastore for propogation to other DNS nameserver nodes.
    '''
    def __loadZones(self):
        if not self.__silent:
            print "Loading zonefile..."
        records = {}
        for line in self.__zoneLines():

            rr = None
            try:
                rr = self.__parseLine(line)
            except Exception:
                print("Some error prevented parsing line: %s" %line)
            prefix_key = self._getPrefixKey(rr.name.name, rr.type)

            self.__addLocalStorage(rr, prefix_key)

        return records

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

    '''
    func __parseLines() processes lines in the zonefile, parsing them into
    resource records and returning the resource record given by a zonefile line.
    '''
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
                            payload=payload,
                            ttl=0) # set TTL to 0 for now so that we can
                            # demonstrate queries without a querier's DNS RR
                            # cache interfering with our tests of independent
                            # queries in our tests.

    '''
    func __recordLookup() looks up RRs that match a query.
    '''
    def __recordLookup(self, query):
        qname = query.name.name
        qtype = query.type

        answer_dict = {}
        authority = []
        additional = []

        prefix_key = self._getPrefixKey(qname, qtype)
        local_matches = None

        with self.__lock:
            local_matches = self.__rr_local.rawData().get(prefix_key, None)

        global num_responses
        num_responses += 1
        print num_responses

        if local_matches is None: # corresponding RRs have been found
            if not self.__silent:
                print "DomainError: %s" % qname
            return error.DomainError
        
        return local_matches, authority, additional

    '''
    func query() returns RRs that match a query.
    '''
    def query(self, query, timeout=None):
        query_result = self.__recordLookup(query)
        self.__count += 1
        print self.__count
        if query_result is not error.DomainError:
            return defer.succeed(query_result)
        else:
            return defer.fail(error.DomainError())


if __name__ =='__main__':
    # Parse args; node name from dns-config and zonefile are required.
    parser = argparse.ArgumentParser(description='Start up node in DNS cluster')
    parser.add_argument('-n', '--node', required=True,
                        help='this node\'s id')
    parser.add_argument('-z', '--zone', required=True,
                        help='this node\'s zonefile')
    parser.add_argument('-c', '--config', required=True,
                        help='this DNS server cluster\'s config file')
    parser.add_argument('-s', '--silent', action='store_true', default=False,
                        help='suppress print statements')
    args = parser.parse_args()

    # Start resolver
    resolver = Resolver(args.node, args.zone, args.config, args.silent)
    # get port for resolver
    port = resolver.getQueryPort()

    factory = server.DNSServerFactory(
        clients=[resolver]
    )
    protocol = dns.DNSDatagramProtocol(controller=factory)

    # Run the server on provided port.
    reactor.listenUDP(port, protocol)
    reactor.listenTCP(port, factory)

    if not args.silent:
        print "Starting DNS server on port %d..." % port
    reactor.run()
