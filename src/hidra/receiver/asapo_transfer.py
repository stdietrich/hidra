from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import re

from hidra import Transfer
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


def get_entry(dict_obj, name):
    try:
        return dict_obj[name]
    except KeyError:
        logger.defug(f"Missing entry for {name} in matched result")
        raise


def parse_file_path(file_regex, file_path):
    # parse file name
    search = re.search(file_regex, file_path)
    if search:
        return search.groupdict()
    else:
        logger.debug("file name: %s", file_path)
        logger.debug("file_regex: %s", file_regex)
        raise Ignored(
            "Does not match file pattern. Ignoring file {}".format(file_path))


class Ignored(Exception):
    """Raised when an event is processed that should be ignored."""
    pass


class AsapoTransfer:
    def __init__(self, endpoint, beamtime, token, n_threads, file_regex,
                 default_data_source=None, timeout=5, beamline='auto',
                 start_file_idx=1):

        self.signal_host = "localhost"
        self.targets = [["localhost", "50101", 1]]

        self.asapo_worker = AsapoWorker(endpoint, beamtime, token, n_threads, file_regex,
                                        default_data_source, timeout, beamline,
                                        start_file_idx)

        self.start_transfer()

    def start_transfer(self):

        query = Transfer("STREAM_METADATA", self.signal_host)
        query.initiate(self.targets)
        query.start()
        self.run(query)
        query.stop()

    def run(self, query):
        while True:
            try:
                [metadata, data] = query.get()
            except:
                break

            if metadata is not None and data is not None:
                local_path = f'{metadata["relative_path"]}/{metadata["filename"]}'
                self.asapo_worker.send_message(local_path, metadata)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Transfer files metadata to ASAPO')
    parser.add_argument('--endpoint', type=str, help='ASAPO produces endpoint',
                        default='localhost:8400')
    parser.add_argument('--beamtime', type=str,
                        help='ASAPO produces beamtime',
                        default='asapo_test')
    parser.add_argument('--beamline', type=str,
                        help='ASAPO produces beamline',
                        default='auto')
    parser.add_argument('--default-data-source', type=str, help='ASAPO data_source',
                        default='test')
    #parser.add_argument('--stream', type=str, help='ASAPO stream. If not given timestamp is used.')
    parser.add_argument('--token', type=str, help='ASAPO produces token',
                        default='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJjMWY2OG0ydWlkZDE3dWxmaDN1ZyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJyZWFkIl19fQ.zo7ZDfY2sf4o9RYuXpxNR9kHLG594xr-SE5yLoyDC2Q')
    parser.add_argument('--n_threads', type=int, help='Number of threds for ASAPO producer',
                        default=5)
    parser.add_argument('--start_file_idx', type=int, help='Starting file index',
                        default=1)
    parser.add_argument('--file_regex', type=str, help='Template to file path, which includes `stream` and `file_idx`',
                        default=".*/(?P<scan_id>.*)/(?P<file_idx_in_scan>.*).h5")
    parser.add_argument('--timeout', type=float, help='ASAPO send timeout in [s]', default=0.5)

    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=logging.DEBUG)
    args = vars(parser.parse_args())

    asapo_transfer = AsapoTransfer(**args)


if __name__ == "__main__":
    main()
