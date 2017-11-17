from pysyncobj import SyncObj
from pysyncobj.batteries import ReplCounter, ReplDict

counter1 = ReplCounter()
counter2 = ReplCounter()
dict1 = ReplDict()
syncObj = SyncObj('127.0.0.1:4322', ['127.0.0.1:4321'], consumers=[counter1, counter2, dict1])

dict1.set('testKey1', 'testValue1', sync=True)
print(counter1, counter2, dict1.get('testKey1'), dict1.get('testKey2'))

