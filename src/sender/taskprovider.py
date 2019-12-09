# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module implements the task provider.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

import errno
from importlib import import_module
import json
import os
import signal
import zmq

from base_class import Base
import hidra.utils as utils

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TaskProvider(Base):
    """
    Combines the events found with the configured event detector with the
    requests gotten by the signal handler.
    """

    def __init__(self,
                 config,
                 endpoints,
                 log_queue):

        super().__init__()

        self.config = config
        self.endpoints = endpoints
        self.log_queue = log_queue

        self.log = None
        self.eventdetector = None

        self.context = None
        self.request_fw_socket = None
        self.router_socket = None
        self.control_socket = None
        self.poller = None
        self.timeout = None

        self.eventdetector = None
        self.keep_running = None
        self.stopped = None
        self.ignore_accumulated_events = None

        self.run()

    def _setup(self):
        """Initializes parameters and creates sockets.
        """

        self.log = utils.get_logger(self.__class__.__name__, self.log_queue)
        self.log.debug("%s started (PID %s).",
                       self.__class__.__name__, os.getpid())

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        try:
            self.timeout = self.config["general"]["taskprovider_timeout"]
        except KeyError:
            self.timeout = 1000
        self.log.debug("Set timeout to %s ms", self.timeout)

        # remember if the context was created outside this class or not
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        try:
            self.ignore_accumulated_events = (
                self.config["eventdetector"]["ignore_accumulated_events"]
            )
        except KeyError:
            self.ignore_accumulated_events = False

        self.log.info("Loading event detector: %s",
                      self.config["eventdetector"]["type"])
        eventdetector_m = import_module(self.config["eventdetector"]["type"])

        self.eventdetector = eventdetector_m.EventDetector(
            {
                "config": self.config,
                "log_queue": self.log_queue,
                "check_dep": True
            }
        )

        self.keep_running = True

        try:
            self.create_sockets()
        except Exception:
            self.log.error("Cannot create sockets", exc_info=True)
            self.stop()

    def create_sockets(self):
        """Create ZMQ sockets.
        """

        # socket to get control signals from
        self.control_socket = self.start_socket(
            name="control_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_socket.setsockopt_string(zmq.SUBSCRIBE, "control")

        # socket to get forwarded requests
        self.request_fw_socket = self.start_socket(
            name="request_fw_socket",
            sock_type=zmq.REQ,
            sock_con="connect",
            endpoint=self.endpoints.request_fw_con,
            socket_options=[[zmq.RCVTIMEO, self.timeout]]
        )

        # socket to distribute the events to the worker
        self.router_socket = self.start_socket(
            name="router_socket",
            sock_type=zmq.PUSH,
            sock_con="bind",
            endpoint=self.endpoints.router_bind,
            # this sometimes blocks indefinitely if there are problems
            # with sending (e.g. when wrong config on datadispatcher)
            socket_options=[[zmq.SNDTIMEO, self.timeout]]
        )

        self.poller = zmq.Poller()
        self.poller.register(self.control_socket, zmq.POLLIN)

    def run(self):
        """Wrapper around the _run method to detect if it has stopped.
        """
        try:
            self._setup()
        except Exception:
            # to make sure that all sockets are closed
            self.stop()
            raise

        self.stopped = False
        try:
            self._run()
        except zmq.ZMQError:
            pass
        except KeyboardInterrupt:
            pass
        except Exception:
            self.log.error("Stopping due to unknown error condition.",
                           exc_info=True)
        finally:
            # ensure that the stop method always knows that the run method
            # actually stopped.
            self.stopped = True
            self.stop()

    def _run(self):
        """Reacts on events and combines them to external signals.
        """

        while self.keep_running:

            # ----------------------------------------------------------------
            # get events
            # ----------------------------------------------------------------

            # the event for a file /tmp/test/source/local/file1.tif
            # is of the form:
            # {
            #   "source_path": "/tmp/test/source/"
            #   "relative_path": "local"
            #   "filename": "file1.tif"
            # }
            try:
                workload_list = self.eventdetector.get_new_event()
            except KeyboardInterrupt:
                break
            except IOError as excp:
                if excp.errno == errno.EINTR:
                    break
                else:
                    self.log.error("Invalid workload message received.",
                                   exc_info=True)
                    workload_list = []
            except Exception:
                self.log.error("Invalid workload message received.",
                               exc_info=True)
                workload_list = []

            # TODO validate workload dict
            for workload in workload_list:

                if not self.keep_running:
                    break

                # ------------------------------------------------------------
                # get requests for this event
                # ------------------------------------------------------------
                requests = ["None"]  # default
                try:
                    self.log.debug("Get requests...")
                    self.request_fw_socket.send_multipart(
                        [b"GET_REQUESTS",
                         json.dumps(workload["filename"]).encode("utf-8")]
                    )

                    requests = json.loads(self.request_fw_socket.recv_string())

                except TypeError:
                    # This happens when CLOSE_FILE is sent as workload
                    pass
                except zmq.error.Again:
                    self.log.debug("Error when getting requests due to "
                                   "timeout of request_socket")
                except Exception:
                    self.log.error("Get Requests... failed.", exc_info=True)

                # ------------------------------------------------------------
                # build message dict
                # ------------------------------------------------------------
                try:
                    self.log.debug("Building message dict...")
                    # set correct escape characters
                    message_dict = json.dumps(workload).encode("utf-8")
                except Exception:
                    self.log.error("Unable to assemble message dict.",
                                   exc_info=True)
                    continue

                # ------------------------------------------------------------
                # send the file to the dataDispatcher
                # ------------------------------------------------------------
                try:
                    self.log.debug("Sending message...")
                    message = [message_dict]
                    if requests != ["None"]:
                        message.append(json.dumps(requests).encode("utf-8"))
                    self.log.debug(str(message))

                    while True:
                        try:
                            self.router_socket.send_multipart(message)
                            break
                        except zmq.error.Again:
                            self.log.warning("Sending message failed due to "
                                             "timeout of router_socket")
                            # if there is a control signal in the meantime this
                            # would otherwise get struck
                            if self._check_control_socket():
                                break
                except Exception:
                    self.log.error("Sending message...failed.", exc_info=True)
                    raise

            # ----------------------------------------------------------------
            # control commands
            # ----------------------------------------------------------------
            if self._check_control_socket():
                break

    def _check_control_socket(self):
        """Check if any control signal where received over the control socket

        Returns:
            A boolean indicating if the class should be stopped or not
            (True means stop).
        """

        socks = dict(self.poller.poll(0))
        if (self.control_socket in socks
                and socks[self.control_socket] == zmq.POLLIN):

            # the exit signal should become effective
            if self.check_control_signal():
                return True

        return False

    def _react_to_exit_signal(self):
        """Overwrite the base class reaction method to exit signal.

        Reaction to exit signal from control socket.
        """
        self.log.debug("Requested to shut down.")
        self.keep_running = False

    def _react_to_wakeup_signal(self, message):
        """Overwrite the base class reaction method to wakeup signal.
        """

        # cleanup accumulated events
        if self.ignore_accumulated_events:
            try:
                acc_events = self.eventdetector.get_new_event()
                self.log.debug("Ignore accumulated workload: %s", acc_events)
            except Exception:
                self.log.error("Invalid workload message "
                               "received.", exc_info=True)

    def stop(self):
        """close sockets and clean up
        """

        self.keep_running = False
        self.wait_for_stopped()

        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        self.stop_socket(name="router_socket")
        self.stop_socket(name="request_fw_socket")
        self.stop_socket(name="control_socket")

        if self.context is not None:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

    # pylint: disable=unused-argument
    def signal_term_handler(self, signal_to_react, frame):
        """React to external SIGTERM signal.
        """

        self.log.debug('got SIGTERM')
        self.keep_running = False
        # calling stop here would set keep_running to False but this would not
        # be propagated to _run because stop would block the thread
        # by setting keep_running explicitly the main loop is given the
        # possibility to stop by itself

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()
