#!/usr/bin/env python

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import argparse
import zmq
import zmq.devices
import os
import logging
import errno
import sys
import json
import time
import cPickle
from multiprocessing import Process, freeze_support, Queue
import ConfigParser
import threading
import signal
import setproctitle

from SignalHandler import SignalHandler
from TaskProvider import TaskProvider
from DataDispatcher import DataDispatcher

try:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH   = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = BASE_PATH + os.sep + "src" + os.sep + "shared"
CONFIG_PATH = BASE_PATH + os.sep + "conf"

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

from logutils.queue import QueueHandler
import helpers
from version import __version__


def argumentParsing():
    defaultConfig     = CONFIG_PATH + os.sep + "dataManager.conf"
    supportedEDTypes  = ["inotifyxdetector", "watchdogdetector", "zmqdetector", "httpdetector"]
    supportedDFTypes  = ["getfromfile", "getfromzmq", "getfromhttp"]

    ##################################
    #   Get command line arguments   #
    ##################################

    parser = argparse.ArgumentParser()

    parser.add_argument("--configFile"        , type    = str,
                                                help    = "Location of the configuration file")

    parser.add_argument("--logfilePath"       , type    = str,
                                                help    = "Path where the logfile will be created")
    parser.add_argument("--logfileName"       , type    = str,
                                                help    = "Filename used for logging")
    parser.add_argument("--logfileSize"       , type    = int,
                                                help    = "File size before rollover in B (linux only)")
    parser.add_argument("--verbose"           , help    = "More verbose output",
                                                action  = "store_true")
    parser.add_argument("--onScreen"          , type    = str,
                                                help    = "Display logging on screen (options are CRITICAL, ERROR, WARNING, INFO, DEBUG)",
                                                default = False )

    parser.add_argument("--procname"          , type    = str,
                                                help    = "Name with which the service should be running")

    # SignalHandler config

    parser.add_argument("--comPort"           , type    = str,
                                                help    = "Port number to receive signals")
    parser.add_argument("--whitelist"         , type    = str,
                                                help    = "List of hosts allowed to connec")

    parser.add_argument("--requestPort"       , type    = str,
                                                help    = "ZMQ port to get new requests")
    parser.add_argument("--requestFwPort"     , type    = str,
                                                help    = "ZMQ port to forward requests")
    parser.add_argument("--controlPubPort"    , type    = str,
                                                help    = "Port number to publish control signals")
    parser.add_argument("--controlSubPort"    , type    = str,
                                                help    = "Port number to receive control signals")

    # EventDetector config

    parser.add_argument("--eventDetectorType" , type    = str,
                                                help    = "Type of event detector to use")
    parser.add_argument("--fixSubdirs"        , type    = str,
                                                help    = "Subdirectories to be monitored and to store data to \
                                                           (only needed if eventDetector is InotifyxDetector or WatchdogDetector \
                                                           and dataFetcher is getFromFile)")

    parser.add_argument("--monitoredDir"      , type    = str,
                                                help    = "Directory to be monitor for changes; inside this directory only the specified \
                                                           subdirectories are monitred (only needed if eventDetector is InotifyxDetector \
                                                           or WatchdogDetector)")
    parser.add_argument("--monitoredEvents"   , type    = str,
                                                help    = "Event type of files (options are: IN_CLOSE_WRITE, IN_MOVED_TO, ...) and \
                                                           the formats to be monitored, files in an other format will be be neglected \
                                                           (needed if eventDetector is InotifyxDetector or WatchdogDetector)")

    parser.add_argument("--historySize"       , type    = int,
                                                help    = "Number of events stored to look for doubles \
                                                           (needed if eventDetector is InotifyxDetector)")

    parser.add_argument("--useCleanUp"        , type    = bool,
                                                help    = "Flag describing if a clean up thread which regularly checks \
                                                           if some files were missed should be activated \
                                                           (needed if eventDetector is InotifyxDetector)")

    parser.add_argument("--actionTime"        , type    = float,
                                                help    = "Intervall time (in seconds) used for clea nup \
                                                           (only needed if eventDetectorType is InotifyxDetector)")

    parser.add_argument("--timeTillClosed"    , type    = float,
                                                help    = "Time (in seconds) since last modification after which a file will be seen as closed \
                                                           (only needed if eventDetectorType is InotifyxDetector (for clean up) or WatchdogDetector)")


    parser.add_argument("--eventPort"         , type    = str,
                                                help    = "ZMQ port to get events from \
                                                           (only needed if eventDetectorType is ZmqDetector)")

    parser.add_argument("--detectorDevice"    , type    = str,
                                                help    = "Tango device proxy for the detector \
                                                           (only needed if eventDetectorType is HttpDetector)")
    parser.add_argument("--filewriterDevice"  , type    = str,
                                                help    = "Tango device proxy for the filewriter \
                                                           (only needed if eventDetectorType is HttpDetector)")

    # DataFetcher config

    parser.add_argument("--dataFetcherType"   , type    = str,
                                                help    = "Module with methods specifying how to get the data)")
    parser.add_argument("--dataFetcherPort"   , type    = str,
                                                help    = "If 'getFromZmq is specified as dataFetcherType it needs a port to listen to)")

    parser.add_argument("--useDataStream"     , type    = str,
                                                help    = "Enable ZMQ pipe into storage system (if set to false: the file is moved \
                                                           into the localTarget)")
    parser.add_argument("--fixedStreamHost"   , type    = str,
                                                help    = "Fixed host to send the data to with highest priority \
                                                           (only active if useDataStream is set)")
    parser.add_argument("--fixedStreamPort"   , type    = str,
                                                help    = "Fixed port to send the data to with highest priority \
                                                           (only active if useDataStream is set)")
    parser.add_argument("--numberOfStreams"   , type    = int,
                                                help    = "Number of parallel data streams)")
    parser.add_argument("--chunkSize"         , type    = int,
                                                help    = "Chunk size of file-parts getting send via ZMQ)")

    parser.add_argument("--routerPort"        , type    = str,
                                                help    = "ZMQ-router port which coordinates the load-balancing \
                                                           to the worker-processes)")

    parser.add_argument("--localTarget"       , type    = str,
                                                help    = "Target to move the files into)")

    parser.add_argument("--storeData"         , type    = bool,
                                                help    = "Flag describing if the data should be stored in localTarget \
                                                           (needed if dataFetcherType is getFromFile or getFromHttp)")
    parser.add_argument("--removeData"        , type    = bool,
                                                help    = "Flag describing if the files should be removed from the source \
                                                           (needed if dataFetcherType is getFromHttp)")

    arguments                    = parser.parse_args()
    arguments.configFile         = arguments.configFile or defaultConfig

    # check if configFile exist
    helpers.checkFileExistance(arguments.configFile)

    ##################################
    # Get arguments from config file #
    ##################################

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(arguments.configFile)))

    # Configure logfile
    arguments.logfilePath        = arguments.logfilePath        or config.get('asection', 'logfilePath')
    arguments.logfileName        = arguments.logfileName        or config.get('asection', 'logfileName')

    if not helpers.isWindows():
        arguments.logfileSize    = arguments.logfileSize        or config.getint('asection', 'logfileSize')

    arguments.procname           = arguments.procname           or config.get('asection', 'procname')

    arguments.comPort            = arguments.comPort            or config.get('asection', 'comPort')
    try:
        arguments.whitelist      = arguments.whitelist          or json.loads(config.get('asection', 'whitelist'))
    except:
        arguments.whitelist      = json.loads(config.get('asection', 'whitelist').replace("'", '"'))

    arguments.requestPort        = arguments.requestPort        or config.get('asection', 'requestPort')

    if helpers.isWindows():
        arguments.requestFwPort  = arguments.requestFwPort      or config.get('asection', 'requestFwPort')

        arguments.controlPubPort = arguments.controlPubPort     or config.get('asection', 'controlPubPort')
        arguments.controlSubPort = arguments.controlSubPort     or config.get('asection', 'controlSubPort')

        arguments.routerPort     = arguments.routerPort         or config.get('asection', 'routerPort')


    arguments.eventDetectorType  = arguments.eventDetectorType  or config.get('asection', 'eventDetectorType')

    if arguments.eventDetectorType == "InotifyxDetector":
        # for InotifyxDetector and WatchdogDetector and getFromFile:
        try:
            arguments.fixSubdirs         = arguments.fixSubdirs     or json.loads(config.get('asection', 'fixSubdirs'))
        except:
            arguments.fixSubdirs     = json.loads(config.get('asection', 'fixSubdirs').replace("'", '"'))
        arguments.monitoredDir       = arguments.monitoredDir       or config.get('asection', 'monitoredDir')
        try:
            arguments.monitoredEvents = arguments.monitoredEvents   or json.loads(config.get('asection', 'monitoredEvents'))
        except:
            arguments.monitoredEvents = json.loads(config.get('asection', 'monitoredEvents').replace("'", '"'))
        arguments.historySize        = arguments.historySize        or config.getint('asection', 'historySize')
        arguments.useCleanUp         = arguments.useCleanUp         or config.getboolean('asection', 'useCleanUp')
        if arguments.useCleanUp:
            arguments.actionTime         = arguments.actionTime     or config.getfloat('asection', 'actionTime')
            arguments.timeTillClosed     = arguments.timeTillClosed or config.getfloat('asection', 'timeTillClosed')

    if arguments.eventDetectorType == "WatchdogDetector":
        try:
            arguments.fixSubdirs         = arguments.fixSubdirs     or json.loads(config.get('asection', 'fixSubdirs'))
        except:
            arguments.fixSubdirs     = json.loads(config.get('asection', 'fixSubdirs').replace("'", '"'))
        arguments.monitoredDir       = arguments.monitoredDir       or config.get('asection', 'monitoredDir')
        try:
            arguments.monitoredEvents = arguments.monitoredEvents   or json.loads(config.get('asection', 'monitoredEvents'))
        except:
            arguments.monitoredEvents = json.loads(config.get('asection', 'monitoredEvents').replace("'", '"'))
        arguments.timeTillClosed     = arguments.timeTillClosed     or config.getfloat('asection', 'timeTillClosed')
        arguments.actionTime         = arguments.actionTime         or config.getfloat('asection', 'actionTime')

    if arguments.eventDetectorType == "ZmqDetector":
        arguments.eventPort          = arguments.eventPort          or config.get('asection', 'eventPort')

    if arguments.eventDetectorType == "HttpGetDetector":
        arguments.detectorDevice     = arguments.detectorDevice     or config.get('asection', 'detectorDevice')
        arguments.filewriterDevice   = arguments.filewriterDevice   or config.get('asection', 'filewriterDevice')

    arguments.dataFetcherType    = arguments.dataFetcherType        or config.get('asection', 'dataFetcherType')

    if arguments.dataFetcherType == "getFromFile":
        arguments.fixSubdirs         = arguments.fixSubdirs         or json.loads(config.get('asection', 'fixSubdirs'))

    if arguments.dataFetcherType == "getFromZMQ":
        arguments.dataFetcherPort    = arguments.dataFetcherPort    or config.get('asection', 'dataFetcherPort')

    arguments.useDataStream      = arguments.useDataStream          or config.getboolean('asection', 'useDataStream')

    if arguments.useDataStream:
        arguments.fixedStreamHost    = arguments.fixedStreamHost    or config.get('asection', 'fixedStreamHost')
        arguments.fixedStreamPort    = arguments.fixedStreamPort    or config.get('asection', 'fixedStreamPort')

    arguments.numberOfStreams    = arguments.numberOfStreams        or config.getint('asection', 'numberOfStreams')
    arguments.chunkSize          = arguments.chunkSize              or config.getint('asection', 'chunkSize')

    arguments.storeData          = arguments.storeData              or config.getboolean('asection', 'storeData')

    if arguments.storeData:
        try:
            arguments.fixSubdirs         = arguments.fixSubdirs     or json.loads(config.get('asection', 'fixSubdirs'))
        except:
            arguments.fixSubdirs     = json.loads(config.get('asection', 'fixSubdirs').replace("'", '"'))
        arguments.localTarget        = arguments.localTarget        or config.get('asection', 'localTarget')

    arguments.removeData         = arguments.removeData             or config.getboolean('asection', 'removeData')



    ##################################
    #     Check given arguments      #
    ##################################

    # check if logfile is writable
    helpers.checkLogFileWritable(arguments.logfilePath, arguments.logfileName)


    # check if the eventDetectorType is supported
    helpers.checkEventDetectorType(arguments.eventDetectorType, supportedEDTypes)

    # check if the dataFetcherType is supported
#    helpers.checkDataFetcherType(arguments.dataFetcherType, supportedDFTypes)

    # check if directories exist
    helpers.checkDirExistance(arguments.logfilePath)
    if arguments.monitoredDir:
        helpers.checkDirExistance(arguments.monitoredDir)
        helpers.checkAnySubDirExists(arguments.monitoredDir, arguments.fixSubdirs)
    if arguments.storeData:
        helpers.checkDirExistance(arguments.localTarget)
        helpers.checkAllSubDirExist(arguments.localTarget, arguments.fixSubdirs)


    if arguments.useDataStream:
        helpers.checkPing(arguments.fixedStreamHost)

    return arguments


class DataManager():
    def __init__ (self, logQueue = None):
        self.device           = None
        self.controlPubSocket = None
        self.testSocket       = None
        self.context          = None
        self.extLogQueue      = True
        self.log              = None
        self.logQueueListener = None

        self.localhost        = "127.0.0.1"
        self.extIp            = "0.0.0.0"
        self.ipcPath          = "/tmp/zeromq-data-transfer"

        self.currentPID       = os.getpid()

        arguments = argumentParsing()

        logfilePath           = arguments.logfilePath
        logfileName           = arguments.logfileName
        logfile               = os.path.join(logfilePath, logfileName)
        logsize               = arguments.logfileSize
        verbose               = arguments.verbose
        onScreen              = arguments.onScreen

        if logQueue:
            self.logQueue    = logQueue
            self.extLogQueue = True
        else:
            self.extLogQueue = False

            # Get queue
            self.logQueue    = Queue(-1)


            # Get the log Configuration for the lisener
            if onScreen:
                h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

                # Start queue listener using the stream handler above.
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1, h2)
            else:
                h1 = helpers.getLogHandlers(logfile, logsize, verbose, onScreen)

                # Start queue listener using the stream handler above
                self.logQueueListener = helpers.CustomQueueListener(self.logQueue, h1)

            self.logQueueListener.start()


        # Create log and set handler to queue handle
        self.log = self.getLogger(self.logQueue)

        procname              = arguments.procname
        setproctitle.setproctitle(procname)
        self.log.info("Running as " + str(procname) )

        self.log.info("DataManager started (PID " + str(self.currentPID) + ").")

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        if not os.path.exists(self.ipcPath):
            os.makedirs(self.ipcPath)

        self.comPort          = arguments.comPort
        self.requestPort      = arguments.requestPort

        self.comConId         = "tcp://{ip}:{port}".format(ip=self.extIp,     port=arguments.comPort)
        self.requestConId     = "tcp://{ip}:{port}".format(ip=self.extIp,     port=arguments.requestPort)

        if helpers.isWindows():
            self.log.info("Using tcp for internal communication.")
            self.controlPubConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.controlPubPort)
            self.controlSubConId  = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.controlSubPort)
            self.requestFwConId   = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.requestFwPort)
            self.routerConId      = "tcp://{ip}:{port}".format(ip=self.localhost, port=arguments.routerPort)

            eventDetConStr        = "tcp://{ip}:{port}".format(ip=self.extIp, port=arguments.eventPort)
            dataFetchConStr       = "tcp://{ip}:{port}".format(ip=self.extIp, port=arguments.dataFetcherPort)
        else:
            self.log.info("Using ipc for internal communication.")
            self.controlPubConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlPub")
            self.controlSubConId  = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlSub")
            self.requestFwConId   = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="requestFw")
            self.routerConId      = "ipc://{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="router")

            eventDetConStr        = "ipc://{path}/{id}".format(path=self.ipcPath, id="eventDetConId")
            dataFetchConStr       = "ipc://{path}/{id}".format(path=self.ipcPath, id="dataFetchConId")


        self.whitelist        = arguments.whitelist

        self.useDataStream    = arguments.useDataStream

        if self.useDataStream:
            self.fixedStreamId = "{host}:{port}".format( host=arguments.fixedStreamHost, port=arguments.fixedStreamPort )
        else:
            self.fixedStreamId = None

        self.numberOfStreams  = arguments.numberOfStreams
        self.chunkSize        = arguments.chunkSize

        self.localTarget      = arguments.localTarget

        # Assemble configuration for eventDetector.
        self.log.info("Configured type of eventDetector: " + arguments.eventDetectorType)
        if arguments.eventDetectorType == "InotifyxDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monSubdirs"        : arguments.fixSubdirs,
                    "monEvents"         : arguments.monitoredEvents,
                    "timeout"           : 1,
                    "historySize"       : arguments.historySize,
                    "useCleanUp"        : arguments.useCleanUp,
                    "cleanUpTime"       : arguments.timeTillClosed,
                    "actionTime"        : arguments.actionTime
                    }
        elif arguments.eventDetectorType == "WatchdogDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "monDir"            : arguments.monitoredDir,
                    "monSubdirs"        : arguments.fixSubdirs,
                    "monEvents"         : arguments.monitoredEvents,
                    "timeTillClosed"    : arguments.timeTillClosed,
                    "actionTime"        : arguments.actionTime
                    }
        elif arguments.eventDetectorType == "ZmqDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "context"           : None,
                    "eventDetConStr"    : eventDetConStr,
                    "numberOfStreams"   : self.numberOfStreams
                    }
        elif arguments.eventDetectorType == "HttpDetector":
            self.eventDetectorConfig = {
                    "eventDetectorType" : arguments.eventDetectorType,
                    "detectorDevice"    : arguments.detectorDevice,
                    "filewriterDevice"  : arguments.filewriterDevice,
                    "historySize"       : arguments.historySize
                    }


        # Assemble configuration for dataFetcher
        self.log.info("Configured Type of dataFetcher: " + arguments.dataFetcherType)
        if arguments.dataFetcherType == "getFromFile":
            self.dataFetcherProp = {
                    "type"            : arguments.dataFetcherType,
                    "fixSubdirs"      : arguments.fixSubdirs,
                    "storeData"       : arguments.storeData,
                    "removeData"      : arguments.removeData
                    }
        elif arguments.dataFetcherType == "getFromZmq":
            self.dataFetcherProp = {
                    "type"            : arguments.dataFetcherType,
                    "context"         : None,
                    "dataFetchConStr" : dataFetchConStr
                    }
        elif arguments.dataFetcherType == "getFromHttp":
            self.dataFetcherProp = {
                    "type"            : arguments.dataFetcherType,
                    "session"         : None,
                    "fixSubdirs"      : arguments.fixSubdirs,
                    "storeData"       : arguments.storeData,
                    "removeData"      : arguments.removeData
                    }


        self.signalHandlerPr  = None
        self.taskProviderPr   = None
        self.dataDispatcherPr = []

        self.log.info("Version: " + str(__version__))

        # IP and DNS name should be both in the whitelist
        helpers.extendWhitelist(self.whitelist, self.log)

        # Create zmq context
        # there should be only one context in one process
#        self.context = zmq.Context.instance()
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

        try:
            if self.testFixedStreamingHost():
                self.createSockets()

                self.run()
        except:
            pass
        finally:
            self.stop()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("DataManager")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def createSockets(self):

        # initiate forwarder for control signals (multiple pub, multiple sub)
        try:
            self.device = zmq.devices.ThreadDevice(zmq.FORWARDER, zmq.SUB, zmq.PUB)
            self.device.bind_in(self.controlPubConId)
            self.device.bind_out(self.controlSubConId)
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
            self.device.start()
            self.log.info("Start thead device forwarding messages from '" + str(self.controlPubConId) + "' to '" + str(self.controlSubConId) + "'")
        except:
            self.log.error("Failed to start thead device forwarding messages from '" + str(self.controlPubConId) + "' to '" + str(self.controlSubConId) + "'", exc_info=True)
            raise


        # socket for control signals
        try:
            self.controlPubSocket = self.context.socket(zmq.PUB)
            self.controlPubSocket.connect(self.controlPubConId)
            self.log.info("Start controlPubSocket (connect): '" + str(self.controlPubConId) + "'")
        except:
            self.log.error("Failed to start controlPubSocket (connect): '" + self.controlPubConId + "'", exc_info=True)
            raise


    def testFixedStreamingHost(self):
        if self.useDataStream:
            try:
                self.testSocket = self.context.socket(zmq.PUSH)
                connectionStr   = "tcp://" + self.fixedStreamId

                self.testSocket.connect(connectionStr)
                self.log.info("Start testSocket (connect): '" + str(connectionStr) + "'")
            except:
                self.log.error("Failed to start testSocket (connect): '" + str(connectionStr) + "'", exc_info=True)
                return False

            try:
                self.log.debug("ZMQ version used: {v}".format(v=zmq.__version__))

                # With older ZMQ versions the tracker results in an ZMQError in
                # the DataDispatchers when an event is processed
                # (ZMQError: Address already in use)
                if zmq.__version__ <= "14.5.0":

                    self.testSocket.send_multipart([b"ALIVE_TEST"])
                    self.log.info("Sending test message to fixed streaming host {id} ... success".format(id=self.fixedStreamId))

                else:

                    tracker = self.testSocket.send_multipart([b"ALIVE_TEST"], copy=False, track=True)
                    if not tracker.done:
                        tracker.wait(2)
                    self.log.debug("tracker.done = {t}".format(t=tracker.done))
                    if not tracker.done:
                        self.log.error("Failed to send test message to fixed streaming host {id}".format(id=self.fixedStreamId), exc_info=True)
                        return False
                    else:
                        self.log.info("Sending test message to fixed streaming host {id} ... success".format(id=self.fixedStreamId))
            except:
                self.log.error("Failed to send test message to fixed streaming host {id}".format(id=self.fixedStreamId), exc_info=True)
                return False

        return True


    def run (self):
        self.signalHandlerPr = threading.Thread ( target = SignalHandler,
                                                  args   = (
                                                      self.controlPubConId,
                                                      self.controlSubConId,
                                                      self.whitelist,
                                                      self.comConId,
                                                      self.requestFwConId,
                                                      self.requestConId,
                                                      self.logQueue,
                                                      self.context
                                                      )
                                                  )
        self.signalHandlerPr.start()

        # needed, because otherwise the requests for the first files are not forwarded properly
        time.sleep(0.5)

        if not self.signalHandlerPr.is_alive():
            return

        self.taskProviderPr = Process ( target = TaskProvider,
                                        args   = (
                                            self.eventDetectorConfig,
                                            self.controlSubConId,
                                            self.requestFwConId,
                                            self.routerConId,
                                            self.logQueue
                                            )
                                        )
        self.taskProviderPr.start()

        for i in range(self.numberOfStreams):
            id = str(i) + "/" + str(self.numberOfStreams)
            pr = Process ( target = DataDispatcher,
                           args   = (
                               id,
                               self.controlSubConId,
                               self.routerConId,
                               self.chunkSize,
                               self.fixedStreamId,
                               self.dataFetcherProp,
                               self.logQueue,
                               self.localTarget
                               )
                           )
            pr.start()
            self.dataDispatcherPr.append(pr)

        while self.signalHandlerPr.is_alive() and self.taskProviderPr.is_alive() and all(dataDispatcher.is_alive() for dataDispatcher in self.dataDispatcherPr):
            time.sleep(1)
            pass

        # notify which subprocess terminated
        if not self.signalHandlerPr.is_alive():
            self.log.info("SignalHandler terminated.")
        if not self.taskProviderPr.is_alive():
            self.log.info("TaskProvider terminated.")
        if not any(dataDispatcher.is_alive() for dataDispatcher in self.dataDispatcherPr):
            self.log.info("One DataDispatcher terminated.")


    def stop (self):

        if self.controlPubSocket:
            self.log.info("Sending 'Exit' signal")
            self.controlPubSocket.send_multipart(["control", "EXIT"])

        # waiting till the other processes are finished
        time.sleep(0.5)

        if self.controlPubSocket:
            self.log.info("Closing controlPubSocket")
            self.controlPubSocket.close(0)
            self.controlPubSocket = None

        if self.testSocket:
            self.log.debug("Stopping testSocket")
            self.testSocket.close(0)
            self.testSocket = None

        if self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

        try:
            controlPubPath = "{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlPub")
            controlSubPath = "{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlSub")
            if os.path.isfile(controlPubPath):
                os.remove("{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlPub"))
            if os.path.isfile(controlPubPath):
                os.remove("{path}/{pid}_{id}".format(path=self.ipcPath, pid=self.currentPID, id="controlSub"))
        except Exception:
            self.log.error("Could not remove remaining ipc sockets", exc_info=True)

        if not self.extLogQueue and self.logQueueListener:
            self.log.info("Stopping logQueue")
            self.logQueue.put_nowait(None)
            self.logQueueListener.stop()
            self.logQueueListener = None


    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()


    def __exit__ (self):
        self.stop()


    def __del__ (self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class Test_Receiver_Stream():
    def __init__(self, comPort, fixedRecvPort, receivingPort, receivingPort2, logQueue):

        self.log = self.getLogger(logQueue)

        context = zmq.Context.instance()

        self.comSocket       = context.socket(zmq.REQ)
        connectionStr   = "tcp://localhost:" + comPort
        self.comSocket.connect(connectionStr)
        self.log.info("=== comSocket connected to " + connectionStr)

        self.fixedRecvSocket = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + fixedRecvPort
        self.fixedRecvSocket.bind(connectionStr)
        self.log.info("=== fixedRecvSocket connected to " + connectionStr)

        self.receivingSocket = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + receivingPort
        self.receivingSocket.bind(connectionStr)
        self.log.info("=== receivingSocket connected to " + connectionStr)

        self.receivingSocket2 = context.socket(zmq.PULL)
        connectionStr   = "tcp://0.0.0.0:" + receivingPort2
        self.receivingSocket2.bind(connectionStr)
        self.log.info("=== receivingSocket2 connected to " + connectionStr)

        self.sendSignal("START_STREAM", receivingPort, 1)
        self.sendSignal("START_STREAM", receivingPort2, 0)

        self.run()


    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def getLogger (self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue) # Just the one handler needed
        logger = logging.getLogger("Test_Receiver_Stream")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger


    def sendSignal (self, signal, ports, prio = None):
        self.log.info("=== sendSignal : " + signal + ", " + str(ports))
        sendMessage = [__version__,  signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["localhost:" + port, prio])
        else:
            targets.append(["localhost:" + ports, prio])

        targets = cPickle.dumps(targets)
        sendMessage.append(targets)
        self.comSocket.send_multipart(sendMessage)
        receivedMessage = self.comSocket.recv()
        self.log.info("=== Responce : " + receivedMessage )

    def run (self):
        try:
            while True:
                recv_message = self.fixedRecvSocket.recv_multipart()
                self.log.info("=== received fixed: " + str(cPickle.loads(recv_message[0])))
                recv_message = self.receivingSocket.recv_multipart()
                self.log.info("=== received: " + str(cPickle.loads(recv_message[0])))
                recv_message = self.receivingSocket2.recv_multipart()
                self.log.info("=== received 2: " + str(cPickle.loads(recv_message[0])))
        except KeyboardInterrupt:
            pass

    def __exit__ (self):
        self.receivingSocket.close(0)
        self.receivingSocket2.close(0)
        context.destroy()


if __name__ == '__main__':
    freeze_support()    #see https://docs.python.org/2/library/multiprocessing.html#windows

    test = False

    if test:
        import time
        from shutil import copyfile
        from subprocess import call


        logfile = BASE_PATH + os.sep + "logs" + os.sep + "dataManager_test.log"
        logsize = 10485760

        logQueue = Queue(-1)

        # Get the log Configuration for the lisener
        h1, h2 = helpers.getLogHandlers(logfile, logsize, verbose=True, onScreenLogLevel="debug")

        # Start queue listener using the stream handler above
        logQueueListener = helpers.CustomQueueListener(logQueue, h1, h2)
        logQueueListener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        root.setLevel(logging.DEBUG) # Log level = DEBUG
        qh = QueueHandler(logQueue)
        root.addHandler(qh)


        comPort        = "50000"
        fixedRecvPort  = "50100"
        receivingPort  = "50101"
        receivingPort2 = "50102"

        testPr = Process ( target = Test_Receiver_Stream, args = (comPort, fixedRecvPort, receivingPort, receivingPort2, logQueue))
        testPr.start()
        logging.debug("test receiver started")

        sourceFile = BASE_PATH + os.sep + "test_file.cbf"
        targetFileBase = BASE_PATH + os.sep + "data" + os.sep + "source" + os.sep + "local" + os.sep + "raw" + os.sep

        try:
            sender = DataManager(logQueue)
        except:
            sender = None

        if sender:
            time.sleep(0.5)
            i = 100
            try:
                while i <= 105:
                    targetFile = targetFileBase + str(i) + ".cbf"
                    logging.debug("copy to " + targetFile)
                    copyfile(sourceFile, targetFile)
                    i += 1

                    time.sleep(1)
            except Exception as e:
                logging.error("Exception detected: " + str(e), exc_info=True)
            finally:
                time.sleep(3)
                testPr.terminate()

                for number in range(100, i):
                    targetFile = targetFileBase + str(number) + ".cbf"
                    try:
                        os.remove(targetFile)
                        logging.debug("remove " + targetFile)
                    except:
                        pass

                sender.stop()
                logQueue.put_nowait(None)
                logQueueListener.stop()

    else:
        sender = None
        try:
            sender = DataManager()
        finally:
            if sender:
                sender.stop()

