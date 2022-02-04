from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import signal
from threading import Event
from time import sleep

from hidra import Transfer, __version__, generate_filepath
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


class Stopped(Exception):
    """Raised when a run is stopped."""
    pass


class AsapoTransfer:
    def __init__(self, asapo_worker, signal_host, detector_id, target_host, target_port, target_dir, reconnect_timout):

        self.signal_host = signal_host
        self.targets = [[target_host, target_port, 1]]
        self.target_dir = target_dir
        self.asapo_worker = asapo_worker
        self.reconnect_timout = reconnect_timout
        logger.info(
            "Creating Transfer instance type=%s signal_host=%s detector_id=%s use_log=True",
            "STREAM_METADATA", self.signal_host, detector_id)
        self.query = Transfer(
            "STREAM_METADATA", self.signal_host, detector_id=detector_id,
            use_log=True)
        self.stop_run = Event()

    def run(self):
        if self.stop_run.is_set():
            raise Stopped
        try:
            logger.info("Initiating connection for %s", self.targets)
            self.query.initiate(self.targets)
            logger.info("Starting query")
            self.query.start()
            self._run()
        finally:
            logger.info("Stopping query")
            self.query.stop()

    def _run(self):
        while not self.stop_run.is_set():
            logger.debug("Querying next message")
            [metadata, data] = self.query.get(timeout=self.reconnect_timout*1000)
            if metadata is not None:
                try:
                    local_path = generate_filepath(self.target_dir, metadata)
                    logger.info("Send file {local_path}".format(local_path=local_path))
                    self.asapo_worker.send_message(local_path, metadata)
                except Exception as e:
                    logger.error("Transmission does not succeed. {e}. File ignored".format(e=str(e)))

    def stop(self):
        logger.info("Runner is stopped.")
        self.stop_run.set()


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
                        default="current/raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5")
    parser.add_argument('--timeout', type=float, help='ASAPO send timeout in [s]', default=30)
    parser.add_argument('--signal_host', type=str, help='Signal host', default='localhost')
    parser.add_argument('--target_host', type=str, help='Target host', default='localhost')
    parser.add_argument('--target_port', type=str, help='Target port', default='50101')
    parser.add_argument('--target_dir', type=str, help='Target directory', default='')
    parser.add_argument('--detector_id', type=str, help='Detector hostname', default='')
    parser.add_argument('--reconnect_timeout', type=int, help='Timeout to reconnect to sender',
                        default=3)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set log level for the application")
    args = vars(parser.parse_args())
    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=getattr(logging, args['log_level']))

    worker_args = dict(
        endpoint=args['endpoint'],
        beamtime=args['beamtime'],
        token=args['token'],
        n_threads=args['n_threads'],
        file_regex=args['file_regex'],
        default_data_source=args['default_data_source'],
        timeout=args['timeout'],
        beamline=args['beamline'],
        start_file_idx=args['start_file_idx'])

    logger.info("Creating AsapoWorker with %s", worker_args)
    asapo_worker = AsapoWorker(**worker_args)

    asapo_transfer = AsapoTransfer(
        asapo_worker=asapo_worker,
        signal_host=args['signal_host'],
        detector_id=args['detector_id'],
        target_host=args['target_host'],
        target_port=args['target_port'],
        target_dir=args['target_dir'],
        reconnect_timeout=args['reconnect_timeout'])

    signal.signal(signal.SIGINT, lambda s, f: asapo_transfer.stop())
    signal.signal(signal.SIGTERM, lambda s, f: asapo_transfer.stop())

    run_transfer(asapo_transfer, args['reconnect_timeout'])


def run_transfer(asapo_transfer, timeout=3):
    while True:
        try:
            asapo_transfer.run()
        except Stopped:
            break
        except Exception:
            logger.warning(
                "Running Transfer stopped with an exception", exc_info=True)
            sleep(timeout)
            logger.info("Retrying connection")


if __name__ == "__main__":
    main()
