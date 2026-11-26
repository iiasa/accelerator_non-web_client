"""Shared test setup."""
import pytest

from accli import internal_certs


@pytest.fixture(autouse=True, scope="session")
def _accli_files_in_temp_dir(tmp_path_factory):
    """Keep accli's certificate files out of the real ~/.accli while tests run.

    accli picks that folder from the account running it (not HOME), so without this the
    tests would write the certificate bundle and saved certificates into the real one.
    """
    base = tmp_path_factory.mktemp("accli-home") / ".accli"
    patch = pytest.MonkeyPatch()
    patch.setattr(internal_certs, "BUNDLE_PATH", base / "ca-bundle.pem")
    patch.setattr(internal_certs, "SAVED_CERTS_DIR", base / "storage-certs")
    patch.setattr(internal_certs, "SERVER_CHECK_FILE", base / "storage-certs" / "last-check.json")
    yield base
    patch.undo()
