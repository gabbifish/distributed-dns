from pysyncobj import SyncObj
from pysyncobj import SyncObjConf
from pysyncobj.batteries import ReplCounter, ReplDict
import time

def printKeys(dict1):
    result1 = dict1.get("testKey1", None)
    result2 = dict1.get("testKey2", None)

    while result1 == None or result2 == None:
        result1 = dict1.get("testKey1", None)
        result2 = dict1.get("testKey2", None)

    print "testKey1 : " + result1
    print "testKey2 : " + result2

dict1 = ReplDict()
config = SyncObjConf(appendEntriesUseBatch=True)
syncObj = SyncObj('127.0.0.1:4323', ['127.0.0.1:4321', '127.0.0.1:4322'], consumers=[dict1], conf=config)


while not syncObj.isReady():
    continue

printKeys(dict1)
