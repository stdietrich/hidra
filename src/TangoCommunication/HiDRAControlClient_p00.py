#!/usr/bin/env python
import os
import sys
import argparse

try:
    # search in global python modules first
    import hidra
except:
    # then search in local modules
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) ) ) )
    API_PATH    = os.path.join(BASE_PATH, "src", "APIs")

    if not API_PATH in sys.path:
        sys.path.append ( API_PATH )
    del API_PATH

    import hidra


def argument_parsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start"   , help    = "Starts the HiDRA Server for the Eiger detector",
                                      action = "store_true")
#    parser.add_argument("--restart" , help    = "Restarts the HiDRA Server for the Eiger detector",
#                                      action = "store_true")
    parser.add_argument("--status"  , help    = "Displays the Status of the HiDRA Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--stop"    , help    = "Stops the HiDRA Server for the Eiger detector",
                                      action = "store_true")
    parser.add_argument("--target"  , type    = str,
                                      help    = "Where to write the data to (default: current/raw; options are: current/raw, current/scratch_bl, commissioning/raw, commissioning/scratch_bl or local)",
                                      default = "current/raw")

    return parser.parse_args()


if __name__ == '__main__':
    arguments = argument_parsing()

    beamline = "p00"
    supportedLocalTargets = ["current/raw", "current/scratch_bl", "commissioning/raw", "commissioning/scratch_bl", "local"]

    if arguments.target and os.path.normpath(arguments.target) not in supportedLocalTargets:
        print "ERROR: target not supported"
        sys.exit(1)

#    obj = hidra.Control(beamline)
    obj = hidra.Control(beamline, useLog=None)

    try:
        if arguments.start:
            obj.set("localTarget", arguments.target)
            obj.set("eigerIp", "131.169.55.170") # lsdma-lab04
            obj.set("eigerApiVersion", "1.5.0")
            obj.set("historySize", 2000)
            obj.set("storeData", False)
            obj.set("removeData", False)
            obj.set("whitelist", "localhost")

            print "Starting HiDRA for Eiger:", obj.do("start")

#        elif arguments.restart:
#            print "restarting HiDRA for Eiger:", obj.do("restart")

        elif arguments.status:
            print "Status of HiDRA for Eiger:", obj.do("status")

        elif arguments.stop:
            print "Stopping HiDRA for Eiger:", obj.do("stop")

    finally:
        obj.stop()

