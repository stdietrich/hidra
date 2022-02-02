from pathlib import Path
import sys
import pytest
from os import path
import logging
from time import time, sleep
from unittest.mock import create_autospec, patch, Mock
import asapo_producer
import threading

receiver_path = (Path(__file__).parent.parent.parent.parent / "src/hidra/receiver")
assert receiver_path.is_dir()
sys.path.insert(0, str(receiver_path))

from plugins.asapo_producer import Plugin, AsapoWorker
from asapo_transfer import AsapoTransfer, run_transfer
from hidra import Transfer


logger = logging.getLogger(__name__)


@pytest.fixture
def config():
    config = dict(
        endpoint="asapo-services:8400",
        beamtime="p00",
        token="abcdefg1234=",
        default_data_source='test001',
        n_threads=1,
        file_regex=".*raw/(?P<scan_id>.*)_(?P<file_idx_in_scan>.*).h5",
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
def file_list():
    file_list = ['current/raw/foo1/bar1_1.h5', 'current/raw/foo1/bar2_1.h5',
                 'current/raw/foo2/bar1_1.h5', 'current/raw/foo2/bar2_1.h5']
    return file_list


@pytest.fixture
def hidra_metadata(file_list):
    hidra_metadata = []
    for file_path in file_list:
        hidra_metadata.append({'source_path': 'http://127.0.0.1/data', 'relative_path': path.dirname(file_path),
                               'filename': path.basename(file_path), 'version': '4.4.2', 'chunksize': 10485760,
                               'file_mod_time': 1643637926.004875,
                               'file_create_time': 1643637926.0048773, 'confirmation_required': False,
                               'chunk_number': 0})
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
    asapo_transfer.query = create_autospec(Transfer, instance=True)
    return_list = [[metadata, None] for metadata in hidra_metadata]
    asapo_transfer.query.get.side_effect = return_list
    yield asapo_transfer


def test_asapo_transfer(caplog, file_list, asapo_transfer):

    x = threading.Thread(target=asapo_transfer.run, args=())
    x.start()
    sleep(1)
    assert f"Send file {file_list[0]}" in caplog.text
    asapo_transfer.stop()
    sleep(1)
    assert "Runner is stopped" in caplog.text
    x.join()


def test_run_transfer(caplog, asapo_transfer):

    x = threading.Thread(target=run_transfer, args=(asapo_transfer, 1))
    x.start()
    sleep(2)
    assert "Retrying connection" in caplog.text
    asapo_transfer.stop()
    sleep(1)
    assert "Runner is stopped" in caplog.text
    x.join()


def test_path_parsing(asapo_transfer, file_list, hidra_metadata):
    asapo_transfer.stop_run.is_set = Mock()
    asapo_transfer.stop_run.is_set.side_effect = [False, False, True]
    asapo_transfer.run()

    asapo_transfer.asapo_worker.send_message.assert_called_with(file_list[0], hidra_metadata[0])

    stream_list = ['foo1/bar1', 'foo1/bar2', 'foo2/bar1', 'foo2/bar2']
    # ToDo: Refactor: Extract free function or so
    for i, file_path in enumerate(file_list):
        data_source, stream, file_idx = asapo_transfer.asapo_worker._parse_file_name(file_path)
        assert file_idx == 1
        assert stream == stream_list[i]
        assert data_source == 'test001'
