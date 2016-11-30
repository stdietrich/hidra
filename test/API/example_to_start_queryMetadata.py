import os
import sys
import time


BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
API_PATH    = os.path.join(BASE_PATH, "src", "APIs")

try:
    # search in global python modules first
    from hidra import Transfer
except:
    # then search in local modules
    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    from hidra import Transfer


signalHost   = "zitpcx19282.desy.de"
#signalHost   = "zitpcx22614w.desy.de"
targets = ["zitpcx19282.desy.de", "50101", 0]
#targets = ["zitpcx22614w.desy.de", "50101", 0]
baseTargetPath = os.path.join(BASE_PATH, "data", "target")
del BASE_PATH

print
print "==== TEST: Query for the newest filename ===="
print

query = Transfer("queryMetadata", signalHost)

query.initiate(targets)

query.start()

while True:
    try:
        [metadata, data] = query.get()
    except:
        break

    print
    print query.generate_target_filepath(baseTargetPath, metadata)
    print
#    time.sleep(0.1)

query.stop()

print
print "==== TEST END: Query for the newest filename ===="
print


