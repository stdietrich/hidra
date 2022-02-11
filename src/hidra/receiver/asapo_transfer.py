from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import signal
import socket
from threading import Event
from time import sleep
import yaml

from hidra import Transfer, __version__, generate_filepath, _constants
from plugins.asapo_producer import AsapoWorker

logger = logging.getLogger(__name__)


class Stopped(Exception):
    """Raised when a run is stopped."""
    pass


class TransferConfig:
    def __init__(self, signal_host, target_host, detector_id,
                 endpoint, beamline, default_data_source,
                 token, target_dir='', n_threads=1, start_file_idx=1, beamtime='auto', target_port=50101,
                 file_regex="current/raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
                 timeout=30, reconnect_timeout=3, log_level="INFO"):
        
        self.detector_id = detector_id
        self.log_level = log_level
        
        self.endpoint = endpoint
        self.beamtime = beamtime
        self.beamline = beamline
        self.token = token
        self.n_threads = n_threads
        self.timeout = timeout
        self.default_data_source = default_data_source
        self.file_regex = file_regex
        self.start_file_idx = start_file_idx
        
        self.signal_host = signal_host
        self.target_port = target_port
        self.target_host = target_host
        self.target_dir = target_dir
        self.reconnect_timeout = reconnect_timeout

    def __repr__(self):
        return str(self.__dict__)

    def get_dict(self):
        return self.config


class AsapoTransfer:
    def __init__(self, asapo_worker, query, target_host, target_port, target_dir, reconnect_timeout):

        self.query = query
        self.targets = [[target_host, target_port, 1]]
        self.target_dir = target_dir
        self.asapo_worker = asapo_worker
        self.reconnect_timeout = reconnect_timeout
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

    args = vars(parser.parse_args())
    config = construct_config(args.pop('config_path'), args.pop('identifier'))
    logging.basicConfig(format="%(asctime)s %(module)s %(lineno)-6d %(levelname)-6s %(message)s",
                        level=getattr(logging, config.log_level))
    logger.info("Start Asapo transfer with parameters %s", config)

    worker_args = dict(
        endpoint=config.endpoint,
        beamtime=config.beamtime,
        token=config.token,
        n_threads=config.n_threads,
        file_regex=config.file_regex,
        default_data_source=config.default_data_source,
        timeout=config.timeout,
        beamline=config.beamline,
        start_file_idx=config.start_file_idx)

    logger.info("Creating AsapoWorker with %s", worker_args)
    asapo_worker = AsapoWorker(**worker_args)

    logger.info(
        "Creating Transfer instance type=%s signal_host=%s detector_id=%s use_log=True",
        "STREAM_METADATA", config.signal_host, config.detector_id)
    query = Transfer("STREAM_METADATA", config.signal_host, detector_id=config.detector_id,
                     use_log=True)

    asapo_transfer = AsapoTransfer(
        asapo_worker=asapo_worker,
        query=query,
        target_host=config.target_host,
        target_port=config.target_port,
        target_dir=config.target_dir,
        reconnect_timeout=config.reconnect_timeout)

    signal.signal(signal.SIGINT, lambda s, f: asapo_transfer.stop())
    signal.signal(signal.SIGTERM, lambda s, f: asapo_transfer.stop())

    run_transfer(asapo_transfer, config.reconnect_timeout)


def construct_config(config_path, identifier):

    # Read config file
    with open(f"{config_path}/asapo_transfer_{identifier}.yaml", "r") as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)

    # parse identifier to extruct `beamline` and `detector_id`
    beamline, detector_id = identifier.split("_", maxsplit=1)
    config['beamline'] = beamline
    config['detector_id'] = detector_id

    if 'default_data_source' not in config:
        config['default_data_source'] = detector_id
        if ".desy.de" in detector_id:
            config['default_data_source'] = detector_id[:detector_id.find(".desy.de")]

    config['target_host'] = socket.getfqdn()
    config['signal_host'] = _constants.CONNECTION_LIST[beamline]['host']

    return TransferConfig(**config)


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
