from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import inspect
import re
import signal
import socket
from threading import Event
from time import sleep
import yaml

from hidra import Transfer, __version__, generate_filepath
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


class Stopped(Exception):
    """Raised when a run is stopped."""
    pass


class TransferConfig:
    def __init__(self, signal_host, target_host, target_dir, detector_id,
                 endpoint, beamline, default_data_source,
                 token, n_threads=1, start_file_idx=1, beamtime='auto', target_port=50101,
                 file_regex="current/raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
                 timeout=30, reconnect_timeout=3, log_level="INFO"):

        frame = inspect.currentframe()
        _, _, _, values = inspect.getargvalues(frame)
        values.pop("self")
        self.config = values


class AsapoTransfer:
    def __init__(self, asapo_worker, signal_host, detector_id, target_host, target_port, target_dir, reconnect_timeout):

        self.signal_host = signal_host
        self.targets = [[target_host, target_port, 1]]
        self.target_dir = target_dir
        self.asapo_worker = asapo_worker
        self.reconnect_timeout = reconnect_timeout
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
            [metadata, data] = self.query.get(timeout=self.reconnect_timeout*1000)
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
    parser.add_argument('--config_path', type=str, help='Path to config directory')
    parser.add_argument(
        'identifier', type=str,
        help='Beamline and detector ID information separated by a single underscore')
    parser.add_argument('--endpoint', type=str, help='ASAPO produces endpoint')
    parser.add_argument('--beamtime', type=str, help='ASAPO produces beamtime')
    parser.add_argument('--beamline', type=str, help='ASAPO produces beamline')
    parser.add_argument('--default-data-source', type=str, help='ASAPO data_source')
    parser.add_argument('--token', type=str, help='ASAPO produces token')
    parser.add_argument('--n_threads', type=int, help='Number of threds for ASAPO producer')
    parser.add_argument('--start_file_idx', type=int, help='Starting file index')
    parser.add_argument('--file_regex', type=str, help='Template to file path, which includes `stream` and `file_idx`')
    parser.add_argument('--timeout', type=float, help='ASAPO send timeout in [s]')
    parser.add_argument('--signal_host', type=str, help='Signal host')
    parser.add_argument('--target_host', type=str, help='Target host')
    parser.add_argument('--target_port', type=str, help='Target port')
    parser.add_argument('--target_dir', type=str, help='Target directory')
    parser.add_argument('--detector_id', type=str, help='Detector hostname')
    parser.add_argument('--reconnect_timeout', type=int, help='Timeout to reconnect to sender')
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set log level for the application")

    args = vars(parser.parse_args())
    args = construct_config(args.pop('config_path'), args.pop('identifier'), args)
    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=getattr(logging, args['log_level']))
    logger.info("Start Asapo transfer with parameters %s", args)

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


def construct_config(config_path, identifier, args):

    # Read config file
    with open(f"{config_path}/asapo_transfer_{identifier}.yaml", "r") as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)

    # parse identifier to extruct `beamline` and `detector_id`
    beamline, detector_id = identifier.split("_", maxsplit=1)
    config['beamline'] = beamline
    config['detector_id'] = detector_id

    if 'default_data_source' not in config:
        config['default_data_source'] = f"hidra_{config['detector_id']}"

    config['target_host'] = socket.getfqdn()
    config.update({k: v for k, v in args.items() if v})

    transfer_config = TransferConfig(**config)

    return transfer_config.config


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
