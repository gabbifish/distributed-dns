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
syncObj = SyncObj('127.0.0.1:4322', ['127.0.0.1:4321', '127.0.0.1:4323'], consumers=[dict1], conf=config)

# dict1.set('testKey1', 'testValue1', sync=True)
# print(syncObj.waitBinded())
# print(syncObj.isReady())


while not syncObj.isReady():
    continue

printKeys(dict1)



# dict1['testKey1'] = 'testValue1'
# print(dict1, dict1.get('testKey1'), dict1.get('testKey2'))
#
