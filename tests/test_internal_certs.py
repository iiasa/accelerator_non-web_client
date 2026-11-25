"""Tests for the IIASA internal storage certificate handling (accli/internal_certs.py)."""
import os
import re
import shutil
import ssl
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from accli import internal_certs

PACKAGE_DIR = Path(internal_certs.__file__).parent


class TestCertificateFile(unittest.TestCase):
    def test_bundled_certificate_matches_expected_fingerprint(self):
        self.assertEqual(internal_certs.fingerprint(internal_certs.cert_path()), internal_certs.CERT_SHA1)

    def test_certificate_is_for_storage_host(self):
        der = ssl.PEM_cert_to_DER_cert(internal_certs.cert_path().read_text())
        self.assertIn(internal_certs.CERT_HOST.encode(), der)

    def test_setup_script_uses_same_fingerprint(self):
        # The Windows administrator script has its own copy of the fingerprint; it must match.
        script = (PACKAGE_DIR / "setup-nfs.ps1").read_text(encoding="utf-8")
        found = re.search(r'\$expectedThumbprint\s*=\s*"([0-9A-F]+)"', script)
        self.assertIsNotNone(found, "setup-nfs.ps1 no longer defines $expectedThumbprint")
        self.assertEqual(found.group(1), internal_certs.CERT_SHA1)

    def test_tampered_certificate_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_module_dir = Path(tmp)
            tampered = internal_certs.cert_path().read_text().replace("MIID6jCCAtKgAwIBAgIU", "MIID6jCCAtKgAwIBAgIV")
            (fake_module_dir / internal_certs.CERT_FILENAME).write_text(tampered)
            with mock.patch.object(internal_certs, "__file__", str(fake_module_dir / "internal_certs.py")):
                with self.assertRaises(Exception):
                    internal_certs.cert_path()


class TestCertificateBundle(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.system_bundle = self.tmp / "system.pem"
        self.system_bundle.write_text("# pretend system certificates\n")
        self.bundle = self.tmp / "home" / ".accli" / "ca-bundle.pem"
        self.patches = [
            mock.patch.object(internal_certs, "BUNDLE_PATH", self.bundle),
            mock.patch.object(internal_certs, "_USER_SSL_CERT_FILE", str(self.system_bundle)),
            mock.patch.dict(os.environ, {"SSL_CERT_FILE": str(self.system_bundle)}),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        shutil.rmtree(self.tmp)

    def test_bundle_has_system_and_iiasa_certificates(self):
        path = internal_certs.apply_cert_bundle()
        text = path.read_text()
        self.assertIn("pretend system certificates", text)
        self.assertIn(internal_certs.cert_path().read_text().strip(), text)
        self.assertEqual(os.environ["SSL_CERT_FILE"], str(self.bundle))

    def test_second_run_uses_system_list_not_own_bundle(self):
        # After the first run SSL_CERT_FILE points at our own bundle; it must not be fed back in.
        internal_certs.apply_cert_bundle()
        first = self.bundle.read_text()
        internal_certs.apply_cert_bundle()
        self.assertEqual(self.bundle.read_text(), first)
        self.assertEqual(first.count("BEGIN CERTIFICATE"), 1)

    def test_bundle_is_updated_when_system_list_changes(self):
        internal_certs.apply_cert_bundle()
        self.system_bundle.write_text("# updated system certificates\n")
        internal_certs.apply_cert_bundle()
        self.assertIn("updated system certificates", self.bundle.read_text())

    def test_openssl_trusts_storage_certificate_with_bundle(self):
        openssl = shutil.which("openssl")
        if not openssl:
            self.skipTest("openssl command not available")
        path = internal_certs.apply_cert_bundle()
        cert = str(internal_certs.cert_path())
        # Only the given file is used (no system folders), so this proves the bundle itself is enough.
        with_bundle = subprocess.run(
            [openssl, "verify", "-no-CApath", "-no-CAstore", "-CAfile", str(path), cert], capture_output=True)
        without = subprocess.run(
            [openssl, "verify", "-no-CApath", "-no-CAstore", "-CAfile", str(self.system_bundle), cert], capture_output=True)
        self.assertEqual(with_bundle.returncode, 0, with_bundle.stdout + with_bundle.stderr)
        self.assertNotEqual(without.returncode, 0)

    def test_bundle_never_built_from_itself(self):
        # Even with no other system list available, a stale copy of our bundle is not doubled up.
        internal_certs.apply_cert_bundle()
        copy = self.tmp / "copy-of-our-bundle.pem"
        shutil.copyfile(self.bundle, copy)
        with mock.patch.object(internal_certs, "_USER_SSL_CERT_FILE", str(copy)):
            internal_certs.apply_cert_bundle()
        self.assertEqual(self.bundle.read_text().count("BEGIN CERTIFICATE"), 1)


class TestInstallCommands(unittest.TestCase):
    """The Windows and macOS install steps, with the system calls faked."""

    def test_windows_install_runs_elevated_import_into_machine_store(self):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            # The first call is the elevated import; the second is the "is it installed?" check.
            return subprocess.CompletedProcess(cmd, 0, stdout="True" if len(calls) > 1 else "", stderr="")

        temp_dir = tempfile.mkdtemp(prefix="o'neil ")
        try:
            with mock.patch.object(internal_certs.subprocess, "run", side_effect=fake_run), \
                    mock.patch.object(internal_certs.tempfile, "gettempdir", return_value=temp_dir):
                self.assertTrue(internal_certs.windows_install_cert())
        finally:
            shutil.rmtree(temp_dir)

        command = calls[0][-1]
        self.assertIn("-Verb RunAs", command)
        self.assertIn(r"Cert:\LocalMachine\Root", command)
        # An apostrophe in the folder name is doubled once per PowerShell quoting level: ' -> ''''.
        self.assertIn("o" + "'" * 4 + "neil", command)
        self.assertIn(internal_certs.CERT_SHA1, calls[1][-1])

    def test_windows_install_reports_failure_when_prompt_declined(self):
        result = subprocess.CompletedProcess([], 0, stdout="False", stderr="")
        with mock.patch.object(internal_certs.subprocess, "run", return_value=result):
            self.assertFalse(internal_certs.windows_install_cert())

    def test_macos_install_uses_password_dialog_when_not_root(self):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        with mock.patch.object(internal_certs.subprocess, "run", side_effect=fake_run), \
                mock.patch.object(internal_certs.os, "geteuid", return_value=501, create=True):
            self.assertTrue(internal_certs.macos_install_cert())

        self.assertEqual(calls[0][0], "osascript")
        self.assertIn("with administrator privileges", " ".join(calls[0]))
        self.assertIn("/Library/Keychains/System.keychain", calls[0][-1])
        self.assertEqual(calls[1][:2], ["security", "verify-cert"])

    def test_ensure_trusted_does_not_prompt_when_install_false(self):
        with mock.patch.object(internal_certs.sys, "platform", "win32"), \
                mock.patch.object(internal_certs, "windows_cert_installed", return_value=False), \
                mock.patch.object(internal_certs, "windows_install_cert") as install:
            self.assertFalse(internal_certs.ensure_trusted(install=False))
            install.assert_not_called()


class TestRequireStorageCert(unittest.TestCase):
    """The check that copy and mount start run before transferring data."""

    def setUp(self):
        from accli import cli
        self.cli = cli

    def test_already_trusted_passes_without_prompt(self):
        with mock.patch.object(internal_certs, "ensure_trusted", return_value=True) as ensure:
            self.cli.require_storage_cert()
        ensure.assert_called_once_with(install=False)

    def test_installs_when_missing(self):
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False, True]) as ensure:
            self.cli.require_storage_cert()
        self.assertEqual(ensure.call_args_list[-1], mock.call(install=True))

    def test_stops_when_install_declined(self):
        import typer
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False, False]):
            with self.assertRaises(typer.Exit):
                self.cli.require_storage_cert()

    def test_copy_checks_certificate_before_transfer(self):
        import typer
        from typer.testing import CliRunner
        with mock.patch.object(self.cli, "require_storage_cert", side_effect=typer.Exit(7)) as check, \
                mock.patch.object(self.cli, "check_for_updates", return_value=(False, None)):
            result = CliRunner().invoke(self.cli.app, ["copy", "acc://proj/file.txt", "./file.txt"])
        check.assert_called_once()
        self.assertEqual(result.exit_code, 7)


if __name__ == "__main__":
    unittest.main()
