__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


import os
import logging
from inotifyx import binding
from inotifyx.distinfo import version as __version__

constants = {}

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyEvent(object):
    '''
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    '''

    wd = None
    mask = None
    cookie = None
    name = None

    def __init__(self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name

    def __str__(self):
        return '%s: %s' % (self.wd, self.get_mask_description())

    def __repr__(self):
        return '%s(%s, %s, %s, %s)' % (
          self.__class__.__name__,
          repr(self.wd),
          repr(self.mask),
          repr(self.cookie),
          repr(self.name),
        )

    def get_mask_description(self):
        '''
        Return an ASCII string describing the mask field in terms of
        bitwise-or'd IN_* constants, or 0.  The result is valid Python code
        that could be eval'd to get the value of the mask field.  In other
        words, for a given event:

        >>> from inotifyx import *
        >>> assert (event.mask == eval(event.get_mask_description()))
        '''

        parts = []
        for name, value in constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return '|'.join(parts)
        return '0'


# Modification of the inotifyx example found inside inotifyx library
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyxDetector():
    def __init__(self, config):

        self.log                = self.getLogger()

        # check format of config
        checkPassed = True
        if ( not config.has_key("monDir") or
                not config.has_key("monEventType") or
                not config.has_key("monDefSubdirs") or
                not config.has_key("monSuffixes") ):
            self.log.error ("Configuration of wrong format")
            checkPassed = False

        self.wd_to_path   = {}
        self.fd           = binding.init()

        if checkPassed:
            #TODO why is this necessary
            self.paths        = [ config["monDir"] ]

            self.monEventType = config["monEventType"]
            self.monSuffixes  = tuple(config["monSuffixes"])
            self.monSubdirs   = config["monSubdirs"]

            self.add_watch()


    def get_events(self, fd, *args):
        '''
        get_events(fd[, timeout])

        Return a list of InotifyEvent instances representing events read from
        inotify.  If timeout is None, this will block forever until at least one
        event can be read.  Otherwise, timeout should be an integer or float
        specifying a timeout in seconds.  If get_events times out waiting for
        events, an empty list will be returned.  If timeout is zero, get_events
        will not block.
        '''
        return [
          InotifyEvent(wd, mask, cookie, name)
          for wd, mask, cookie, name in binding.get_events(fd, *args)
        ]


    def getLogger(self):
        logger = logging.getLogger("inotifyDetector")
        return logger


    def add_watch(self):
        dirsToRegister=self.getDirectoryStructure()
        try:
#            for path in self.paths:
            for path in dirsToRegister:
                wd = binding.add_watch(self.fd, path)
                self.wd_to_path[wd] = path
                self.log.debug("Register watch for path:" + str(path) )
        except Exception as e:
            self.log.error("Could not register watch for path: " + str(path) )
            self.log.debug("Error was " + str(e))
#            self.stop()


    def getDirectoryStructure(self):
        # Add the default subdirs
        self.log.info("paths:" + str(self.paths))
        dirsToWalk    = [os.path.normpath(self.paths[0] + os.sep + directory) for directory in self.monSubdirs]
        self.log.info("dirsToWalk:" + str(dirsToWalk))
        monitoredDirs = []

        # Walk the tree
        for directory in dirsToWalk:
            if os.path.isdir(directory):
                monitoredDirs.append(directory)
                for root, directories, files in os.walk(directory):
                    # Add the found dirs to the list for the inotify-watch
                    monitoredDirs.append(root)
                    self.log.info("Add directory to monitor: " + str(root))
            else:
                self.log.info("Dir does not exists: " + str(directory))

        return monitoredDirs

    def getNewEvent(self):

        eventMessageList = []
        eventMessage = {}

#        print "wd_to_path: ", self.wd_to_path
#        print "fd:", self.fd
        events = self.get_events(self.fd)
        removedWd = None
        for event in events:

            if not event.name:
                continue

            try:
                path = self.wd_to_path[event.wd]
            except:
                path = removedWd
            parts = event.get_mask_description()
            parts_array = parts.split("|")

#            print path, event.name, parts
#            print event.name

            is_dir        = ("IN_ISDIR" in parts_array)
            is_created    = ("IN_CREATE" in parts_array)
            is_moved_from = ("IN_MOVED_FROM" in parts_array)
            is_moved_to   = ("IN_MOVED_TO" in parts_array)

            is_ofEventType = (self.monEventType in parts_array)

            # if a new directory is created or a directory is renamed inside the monitored one,
            # this one has to be monitored as well
            if is_dir and (is_created or is_moved_to):
                dirname = os.path.join(path, event.name)
                self.log.info("Directory event detected: " + str(dirname) + "," + str(parts))
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list: " + str(dirname))
                else:
                    wd = binding.add_watch(self.fd, dirname)
                    self.wd_to_path[wd] = dirname
                    self.log.info("Added new directory to watch:" + str(dirname))

                    # because inotify misses subdirectory creations if they happen to fast,
                    # the newly created directory has to be walked to get catch this misses
                    # http://stackoverflow.com/questions/15806488/inotify-missing-events
                    traversedPath = dirname
                    for root, directories, files in os.walk(dirname):
                        # Add the found dirs to the list for the inotify-watch
                        for dname in directories:
                            traversedPath = os.path.join(traversedPath, dname)
                            wd = binding.add_watch(self.fd, traversedPath)
                            self.wd_to_path[wd] = traversedPath
                            self.log.info("Added new subdirectory to watch:" + str(traversedPath))
                        for filename in files:
                            eventMessage = self.getEventMessage(path, filename)
                            eventMessageList.append(eventMessage)
                continue

            # if a directory is renamed the old watch has to be removed
            if is_dir and is_moved_from:
                dirname = os.path.join(path, event.name)
                for watch, watchPath in self.wd_to_path.iteritems():
                    if watchPath == dirname:
                        foundWatch = watch
                binding.rm_watch(self.fd, foundWatch)
                self.log.info("Removed directory from watch:" + str(dirname))
                # the IN_MOVE_FROM event always apears before the IN_MOVE_TO (+ additional) events
                # and thus has to be stored till loop is finished
                removedWd = self.wd_to_path[foundWatch]
                # removing the watch out of the dictionary cannot be done inside the loop
                # (would throw error: dictionary changed size during iteration)
                del self.wd_to_path[foundWatch]
                continue

            # only files of the configured event type are send
            if not is_dir and is_ofEventType:

#                print path, event.name, parts
#                print event.name

                # only files with end with a suffix specified in monSuffixed are monitored
                if not event.name.endswith(self.monSuffixes):
                    self.log.debug("File ending not in monitored Suffixes: " + str(event.name))
                    self.log.debug("detected events were: " + str(parts))
                    continue


                eventMessage = self.getEventMessage(path, event.name)
                eventMessageList.append(eventMessage)

        return eventMessageList



    def getEventMessage(self, path, filename):

        parentDir    = path
        relativePath = ""
        eventMessage = {}

        # traverse the relative path till the original path is reached
        # e.g. created file: /source/dir1/dir2/test.tif
        splitPath = True
        while splitPath:
            if parentDir not in self.paths:
                (parentDir,relDir) = os.path.split(parentDir)
                # the os.sep is needed at the beginning because the relative path is built up from the right
                # e.g.
                # self.paths = ["/tmp/test/source"]
                # path = /tmp/test/source/local/testdir
                # first iteration: self.monEventType parentDir = /tmp/test/source/local, relDir = /testdir
                # second iteration: parentDir = /tmp/test/source,       relDir = /local/testdir
                relativePath = os.sep + relDir + relativePath
            else:
                # the event for a file /tmp/test/source/local/file1.tif is of the form:
                # {
                #   "sourcePath" : "/tmp/test/source"
                #   "relativePath": "/local"
                #   "filename"   : "file1.tif"
                # }
                eventMessage = {
                        "sourcePath"  : parentDir,
                        "relativePath": relativePath,
                        "filename"    : filename
                        }

                return eventMessage



    def stop(self):
        try:
            for wd in self.wd_to_path:
                try:
                    binding.rm_watch(self.fd, wd)
                except Exception as e:
                    self.log.error("Unable to remove watch: " + wd)
                    self.log.debug("Error was: " + str(e))
        finally:
            os.close(self.fd)

    def __exit__(self):
        self.stop()


    def __del__(self):
        self.stop()


if __name__ == '__main__':
    import sys

    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    SRC_PATH  = BASE_PATH + os.sep + "src"

    sys.path.append ( SRC_PATH )

    import shared.helperScript as helperScript

    logfilePath = BASE_PATH + "/logs/inotifyDetector.log"
    verbose=True

    #enable logging
    helperScript.initLogging(logfilePath, verbose)

    config = {
            "monDir"       : [ BASE_PATH + "/data/source" ],
            "monEventType" : "IN_MOVED_TO",
            "monSubdirs"   : ["local"],
            "monSuffixes"  : [".tif", ".cbf"]
            }

    eventDetector = InotifyDetector(config)

    while True:
        try:
            eventList = eventDetector.getNewEvent()
            if eventList:
                print eventList
        except KeyboardInterrupt:
            break
