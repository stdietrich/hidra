from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
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
    parser.add_argument('--config_file', type=str, help='Path to config file')
    parser.add_argument('identifier', type=str, help='Beamline and detector ID information')

    args = vars(parser.parse_args())
    args = construct_config(args['config_file'], args['identifier'])
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


def construct_config(config_file, identifier):

    # Read config file
    with open(config_file, "r") as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)

    # parse identifier to extruct `beamline` and `detector_id`
    identifier_info = re.search(".*@(?P<beamline>.*)_(?P<detector_id>.*).service", identifier).groupdict()
    config.update(identifier_info)

    config['target_host'] = socket.getfqdn()
    return config


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
