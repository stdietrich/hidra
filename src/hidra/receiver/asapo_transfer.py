from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging

from hidra import Transfer, __version__, generate_filepath
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


class AsapoTransfer:
    def __init__(self, asapo_worker, signal_host, target_host, target_port, target_dir):

        self.signal_host = signal_host
        self.targets = [[target_host, target_port, 1]]
        self.target_dir = target_dir
        self.asapo_worker = asapo_worker
        self.query = Transfer("STREAM_METADATA", self.signal_host)
        self.query.initiate(self.targets)

    def start_transfer(self):
        self.query.start()
        self.run()

    def run(self):
        while True:
            try:
                [metadata, data] = self.query.get()
                if metadata is not None and data is not None:
                    local_path = generate_filepath(self.target_dir, metadata)
                    logger.info("Send file {local_path}".format(local_path=local_path))
                    self.asapo_worker.send_message(local_path, metadata)
            except Exception as e:
                logger.error("Transmission does not succeed. {e}. File ignored".format(e=str(e)))

    def stop(self):
        self.query.stop()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='Transfer files metadata to ASAPO')
    parser.add_argument('--endpoint', type=str, help='ASAPO produces endpoint')
    parser.add_argument('--beamtime', type=str, help='ASAPO produces beamtime')
    parser.add_argument('--beamline', type=str, help='ASAPO produces beamline')
    parser.add_argument('--default-data-source', type=str, help='ASAPO data_source')
    parser.add_argument('--token', type=str, help='ASAPO produces token')
    parser.add_argument('--n_threads', type=int, help='Number of threds for ASAPO producer',
                        default=1)
    parser.add_argument('--start_file_idx', type=int, help='Starting file index',
                        default=1)
    parser.add_argument('--file_regex', type=str, help='Template to file path, which includes `stream` and `file_idx`',
                        default=".*raw/(?P<scan_id>.*)/\w*_(?P<file_idx_in_scan>.*).h5")
    parser.add_argument('--timeout', type=float, help='ASAPO send timeout in [s]', default=30)
    parser.add_argument('--signal_host', type=str, help='Signal host', default='localhost')
    parser.add_argument('--target_host', type=str, help='Target host', default='localhost')
    parser.add_argument('--target_port', type=str, help='Target port', default='50101')
    parser.add_argument('--target_dir', type=str, help='Target directory', default='')
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set log level for the application")
    args = vars(parser.parse_args())
    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=getattr(logging, args['log_level']))

    asapo_worker = AsapoWorker(args['endpoint'], args['beamtime'], args['token'],
                               args['n_threads'], args['file_regex'],
                               args['default_data_source'],
                               args['timeout'], args['beamline'],
                               args['start_file_idx'])
    asapo_transfer = AsapoTransfer(asapo_worker, args['signal_host'], args['target_host'], args['target_port'],
                                   args['target_dir'])
    asapo_transfer.start_transfer()


if __name__ == "__main__":
    main()
