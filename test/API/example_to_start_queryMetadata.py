import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = BASE_PATH + os.sep + "APIs"

if not API_PATH in sys.path:
    sys.path.append ( API_PATH )
del API_PATH
del BASE_PATH

from dataTransferAPI import dataTransfer


signalHost   = "zitpcx19282.desy.de"
#signalHost   = "zitpcx22614w.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]
#targets = ["zitpcx22614w.desy.de", "50101", 0]
basePath = BASE_PATH + os.sep + "data" + os.sep + "target"

print
print "==== TEST: Query for the newest filename ===="
print

query = dataTransfer("queryMetadata", signalHost)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print query.generateTargetFilepath(basePath, metadata)
    print
#    time.sleep(0.1)

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


