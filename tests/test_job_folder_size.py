"""The error dispatch shows when the job folder is too large."""
import re
from unittest import mock

import pytest

from accli import AcceleratorTaskDispatcher as dispatcher

PATH_FORMAT = r"jobstore/\d{8}/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"


def test_job_code_path_format():
    path = dispatcher.job_code_path()
    assert re.fullmatch(PATH_FORMAT, path)
    assert path != dispatcher.job_code_path()  # a new one each time


def test_job_code_path_uses_todays_date():
    with mock.patch.object(dispatcher, "datetime") as fake:
        fake.now.return_value.strftime.return_value = "20261109"
        assert dispatcher.job_code_path().startswith("jobstore/20261109/")
        fake.now.return_value.strftime.assert_called_with("%Y%m%d")


@pytest.fixture
def message(tmp_path, monkeypatch):
    (tmp_path / "big.bin").write_bytes(b"x" * 20)
    monkeypatch.setattr(dispatcher, "MAX_JOB_FOLDER_BYTES", 10)
    with pytest.raises(ValueError) as err:
        dispatcher.check_job_folder_size(str(tmp_path), "/home/me/my job", "myproject")
    return str(err.value)


def test_message_copy_command_uses_the_jobstore_path(message):
    assert re.search(rf'accli copy "/home/me/my job" acc://myproject/{PATH_FORMAT} --exclude "<patterns>"', message)


def test_message_runs_the_job_from_the_same_path(message):
    path = re.search(rf"acc://myproject/({PATH_FORMAT})", message).group(1)
    assert f'command="cd /mnt/wdrv/{path} && <your current command>"' in message
    assert f"acc://myproject/{path} is available in the job at /mnt/wdrv/{path}" in message


def test_message_steps_are_in_order(message):
    positions = [message.index(f"  {n}. ") for n in (1, 2, 3, 4)]
    assert positions == sorted(positions)
    assert "Run 'accli dispatch' again" in message


def test_message_is_generic(message):
    # General guidance only: no lists of build files or folder contents.
    assert 'include="<build files>",' in message
    assert "Look at the job's command in wkube.py" in message
    assert "include wins over exclude" in message
    for specific in ["requirements", "renv", "node_modules", "Largest items"]:
        assert specific not in message


def test_small_folder_passes(tmp_path):
    (tmp_path / "small.txt").write_text("ok")
    dispatcher.check_job_folder_size(str(tmp_path), "./myjob", "myproject")  # no error
