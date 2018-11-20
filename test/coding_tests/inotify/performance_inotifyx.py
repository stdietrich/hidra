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

# the inotifyx library part:
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""Perform tests on the inotifyx library.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import time

from inotifyx import binding

from performance_base import PerformanceBase, do_tests


_constants = {}  # pylint: disable=invalid-name

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = _constants[name] = getattr(binding, name)


class InotifyEvent(object):
    """
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    """
    # pylint: disable=redefined-outer-name
    # pylint: disable=invalid-name

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
        for name, value in _constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return '|'.join(parts)
        return '0'


def get_events(fd, *args):
    """
    get_events(fd[, timeout])

    Return a list of InotifyEvent instances representing events read from
    inotify.  If timeout is None, this will block forever until at least one
    event can be read.  Otherwise, timeout should be an integer or float
    specifying a timeout in seconds.  If get_events times out waiting for
    events, an empty list will be returned.  If timeout is zero, get_events
    will not block.
    """
    # pylint: disable=redefined-outer-name
    # pylint: disable=invalid-name

    return [
        InotifyEvent(wd, mask, cookie, name)
        for wd, mask, cookie, name in binding.get_events(fd, *args)
    ]


class CreateAndGet(PerformanceBase):
    """Create and get events with the inotifyx library.
    """

    def __init__(self, watch_dir, n_files):
        super(CreateAndGet, self).__init__(watch_dir, n_files)
        self.wd_to_path = {}
        self.inotify_binding = binding.init()

        wd = binding.add_watch(self.inotify_binding, self.watch_dir)
        self.wd_to_path[wd] = self.watch_dir

    def run(self):
        """Run the event detection.
        """

        if self.create_job is not None:
            self.create_job.start()

        n_events = 0
        run_loop = True
        t_start = 0
        while run_loop:
            events = get_events(self.inotify_binding, self.timeout)

            if t_start and not events:
                run_loop = False

            for event in events:
                if not t_start:
                    t_start = time.time()

                if event.wd < 0:
                    continue

                event_type = event.get_mask_description()
                event_type_array = event_type.split("|")

                if "IN_OPEN" in event_type_array:
                    n_events += 1

        t_needed = time.time() - t_start
        print("n_events {} in {} s, ({} Hz)"
              .format(n_events, t_needed, n_events / t_needed))

        if self.create_job is not None:
            self.create_job.join()

    def stop(self):
        os.close(self.inotify_binding)


if __name__ == '__main__':
    do_tests(CreateAndGet)
