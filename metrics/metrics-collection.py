#!/usr/bin/env python
import time
import subprocess
import threading
import multiprocessing
import Queue

def startNode(node_id):
    proc_string = "python ../dns-node.py -n %s -z ../zf/zf%s.txt -c ../dns-config.json -a" % (node_id, node_id)
    subprocess.check_output(proc_string, shell=True)

def runDigBenchmark(dig_str, queue):
    start_time = time.time()
    query_count = 0
    while time.time() < start_time+1:
        subprocess.check_output(dig_str, shell=True)
        query_count += 1
    queue.put(query_count)

def runQueryThreads(dig_str):
    num_threads = 2*multiprocessing.cpu_count()+1
    threads = []
    queue = Queue.Queue(num_threads)
    for i in range(0, num_threads):
        new_thread = threading.Thread(target=runDigBenchmark, args=[dig_str, queue])
        threads.append(new_thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    total_queries = 0
    while not queue.empty():
        try:
            total_queries += queue.get()
        except ValueError:
            print "done iterating"

    print "This test recorded a rate of %d queries/sec." % total_queries

if __name__ =='__main__':
    nodes = []
    num_nodes = 3
    for i in range(0, 3):
        new_node = threading.Thread(target=startNode, args=[str(i+1)])
        nodes.append(new_node)
        new_node.setDaemon(True)
        new_node.start()

    time.sleep(11)

    num_threads = 2*multiprocessing.cpu_count()+1

    print "Benchmark 1: Query for record available on queried node's zonefile"
    runQueryThreads("dig -p 10053 @127.0.0.1 example.com MX")

    print "Benchmark 2: Query for record not available on queried node's zonefile (but available on other node's zonefile)"
    runQueryThreads("dig -p 10053 @127.0.0.1 cats.com MX")

    # SIGINT to end this program, for it has just started its own DNS cluster
    # nodes!
