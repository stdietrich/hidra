from pathlib import Path
import sys
import pytest
from os import path
from time import time
from unittest.mock import create_autospec, patch, Mock
import asapo_producer

receiver_path = (Path(__file__).parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, str(receiver_path))

from plugins.asapo_producer import Plugin, AsapoWorker  # noqa
from asapo_transfer import AsapoTransfer


@pytest.fixture
def config():
    config = dict(
        endpoint="asapo-services:8400",
        beamtime="p00",
        token="abcdefg1234=",
        default_data_source='test001',
        n_threads=1,
        file_regex=".*raw/(?P<scan_id>.*)/\w*_(?P<file_idx_in_scan>.*).h5",
        user_config_path="/path/to/config.yaml"
    )
    return config


@pytest.fixture
def transfer_config():
    transfer_config = dict(
        signal_host="localhost",
        target_host="localhost",
        target_port="50101",
        target_dir='',
        reconnect_timeout=1,
    )
    return transfer_config


@pytest.fixture
def hidra_metadata():
    hidra_metadata = {'source_path': 'http://127.0.0.1/data', 'relative_path': 'current/raw/test_file_29',
                      'filename': '0.h5', 'version': '4.4.2', 'chunksize': 10485760, 'file_mod_time': 1643637926.004875,
                      'file_create_time': 1643637926.0048773, 'confirmation_required': False, 'chunk_number': 0}
    return hidra_metadata


@pytest.fixture
def worker(config):
    worker_config = config.copy()
    del worker_config["user_config_path"]
    worker = AsapoWorker(**worker_config)
    worker.send_message = create_autospec(worker.send_message)
    yield worker


@pytest.fixture
def asapo_transfer(worker, transfer_config, hidra_metadata):
    asapo_transfer = AsapoTransfer(worker, transfer_config['signal_host'], transfer_config['target_host'],
                                   transfer_config['target_port'],
                                   transfer_config['target_dir'], transfer_config['reconnect_timeout'])
    asapo_transfer.query.get = create_autospec(asapo_transfer.query.get,
                                               return_value=[hidra_metadata, None])

    yield asapo_transfer


def test_asapo_transfer(caplog, asapo_transfer):
    asapo_transfer.stop_run.is_set = Mock()
    asapo_transfer.stop_run.is_set.side_effect = [False, False, True]
    asapo_transfer.run()
    assert "Send file current/raw/test_file_29/0.h5" in caplog.text
    asapo_transfer.stop()
    assert "Runner is stopped" in caplog.text

