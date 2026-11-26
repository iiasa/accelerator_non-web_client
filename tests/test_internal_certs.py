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
        found = re.search(r'\[string\]\$ExpectedThumbprint\s*=\s*"([0-9A-F]+)"', script)
        self.assertIsNotNone(found, "setup-nfs.ps1 no longer has a default for -ExpectedThumbprint")
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


class TestWhoNeedsIt(unittest.TestCase):
    """Asking the server whether this computer is internal, and whether prompts are possible."""

    def _fake_get(self, status=200, body=None, error=None):
        def get(url, **kwargs):
            if error:
                raise error
            resp = mock.Mock(status_code=status)
            resp.json.side_effect = (lambda: body) if body is not None else ValueError("not JSON")
            return resp
        return mock.patch("requests.get", side_effect=get)

    def test_server_says_internal(self):
        with self._fake_get(body={"internal": True, "storage_host": internal_certs.CERT_HOST}) as get:
            self.assertEqual(internal_certs.cert_needed("https://acc.example", "tok")[0], True)
        self.assertEqual(get.call_args[0][0], "https://acc.example/api/xet-cas/v1/network")
        # The endpoint requires sign-in, so the user's access token is sent.
        self.assertEqual(get.call_args[1]["headers"], {"x-authorization": "tok"})

    def test_server_says_external(self):
        with self._fake_get(body={"internal": False, "storage_host": None}):
            self.assertEqual(internal_certs.cert_needed("https://acc.example", "tok")[0], False)

    def test_internal_storage_with_other_certificate(self):
        with self._fake_get(body={"internal": True, "storage_host": "s3.other.example"}):
            self.assertEqual(internal_certs.cert_needed("https://acc.example", "tok")[0], False)

    def test_unknown_is_treated_as_internal(self):
        # Old server without the endpoint, a web page instead of JSON, or no connection.
        for fake in (self._fake_get(status=404), self._fake_get(status=200), self._fake_get(error=OSError("offline"))):
            with fake:
                self.assertIsNone(internal_certs.storage_network("https://acc.example", "tok"))
                self.assertEqual(internal_certs.cert_needed("https://acc.example", "tok")[0], True)

    def test_cert_needed_does_not_ask_server_twice(self):
        with self._fake_get(error=OSError("offline")) as get:
            internal_certs.cert_needed("https://acc.example", "tok", None)
        get.assert_not_called()

    def test_can_prompt(self):
        tty = mock.Mock(isatty=mock.Mock(return_value=True))
        pipe = mock.Mock(isatty=mock.Mock(return_value=False))
        cases = [
            ({}, tty, True),  # person at a terminal
            ({}, pipe, False),  # AI agent, script, CI
            ({"ACCLI_GUI": "1"}, pipe, True),  # GUI: no terminal, but a person is there
            ({"ACCLI_NO_PROMPT": "1"}, tty, False),  # turned off on purpose
            ({"ACCLI_NO_PROMPT": "1", "ACCLI_GUI": "1"}, pipe, False),
        ]
        for env, stdin, expected in cases:
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(internal_certs.sys, "stdin", stdin):
                for key in {"ACCLI_GUI", "ACCLI_NO_PROMPT"} - set(env):
                    os.environ.pop(key, None)
                self.assertEqual(internal_certs.can_prompt(), expected, env)


class TestRequireStorageCert(unittest.TestCase):
    """The check that downloads and mount start run before transferring data."""

    def setUp(self):
        from accli import cli
        self.cli = cli
        for patch in (mock.patch.object(cli, "get_access_token", return_value="tok"),
                      # The daily server check has its own tests; keep these offline.
                      mock.patch.object(internal_certs, "server_check_due", return_value=False)):
            patch.start()
            self.addCleanup(patch.stop)

    def test_asks_server_for_new_certificate_when_due(self):
        info = {"internal": False, "storage_host": None, "storage_certificate": None}
        with mock.patch.object(internal_certs, "server_check_due", return_value=True), \
                mock.patch.object(internal_certs, "refresh_from_server", return_value=(info, None)) as refresh, \
                mock.patch.object(internal_certs, "ensure_trusted", return_value=False), \
                mock.patch.object(internal_certs, "storage_network") as network:
            self.cli.require_storage_cert("https://acc.example")  # external: nothing required
        refresh.assert_called_once_with("https://acc.example", "tok")
        network.assert_not_called()  # the answer from the refresh is reused

    def _patch(self, trusted, needed=True, can_prompt=True):
        """trusted: list of results for ensure_trusted calls (install=False first, then install=True)."""
        return [
            mock.patch.object(internal_certs, "ensure_trusted", side_effect=trusted),
            mock.patch.object(internal_certs, "cert_needed", return_value=(needed, "Because of a test.")),
            mock.patch.object(internal_certs, "can_prompt", return_value=can_prompt),
        ]

    def _run(self, patches):
        for p in patches:
            p.start()
        try:
            return self.cli.require_storage_cert("https://acc.example")
        finally:
            for p in reversed(patches):
                p.stop()

    def test_already_trusted_passes_without_asking_server(self):
        with mock.patch.object(internal_certs, "ensure_trusted", return_value=True), \
                mock.patch.object(internal_certs, "cert_needed") as needed:
            self.cli.require_storage_cert("https://acc.example")
        needed.assert_not_called()

    def test_not_logged_in_skips_check(self):
        # The command itself then says "please log in" instead of asking for the certificate.
        with mock.patch.object(internal_certs, "ensure_trusted", return_value=False), \
                mock.patch.object(self.cli, "get_server_url", return_value=None), \
                mock.patch.object(internal_certs, "cert_needed") as needed:
            self.cli.require_storage_cert()
        needed.assert_not_called()

    def test_mount_requires_certificate_on_any_network(self):
        # A mount can outlive a move onto the IIASA network, so the network is not checked.
        import typer
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False]), \
                mock.patch.object(internal_certs, "cert_needed") as needed, \
                mock.patch.object(internal_certs, "can_prompt", return_value=False), \
                mock.patch.object(self.cli, "print_cert_action_required") as message:
            with self.assertRaises(typer.Exit) as stop:
                self.cli.require_storage_cert("https://acc.example", always=True)
        needed.assert_not_called()
        self.assertEqual(stop.exception.exit_code, internal_certs.EXIT_NEEDS_PERSON)
        self.assertIn("moves onto the IIASA network", message.call_args[0][0])

    def test_mount_installs_when_person_present(self):
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False, True]) as ensure, \
                mock.patch.object(internal_certs, "can_prompt", return_value=True):
            self.cli.require_storage_cert("https://acc.example", always=True)
        self.assertEqual(ensure.call_args_list[-1], mock.call(install=True))

    def test_help_explains_exit_code_3_to_agents(self):
        from typer.testing import CliRunner
        for args in (["--help"], ["copy", "--help"], ["mount", "start", "--help"], ["cert", "--help"]):
            with mock.patch.object(self.cli, "check_for_updates", return_value=(False, None)):
                out = " ".join(CliRunner().invoke(self.cli.app, args).output.split())
            self.assertIn("exit code 3", out.lower(), args)
            self.assertIn("ACCLI_NO_PROMPT=1", out, args)

    def test_not_needed_outside_iiasa_network(self):
        patches = self._patch([False], needed=False)
        self._run(patches)  # no exception, no install attempt

    def test_installs_when_needed_and_person_present(self):
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False, True]) as ensure, \
                mock.patch.object(internal_certs, "cert_needed", return_value=(True, "r")), \
                mock.patch.object(internal_certs, "can_prompt", return_value=True):
            self.cli.require_storage_cert("https://acc.example")
        self.assertEqual(ensure.call_args_list[-1], mock.call(install=True))

    def test_stops_when_install_declined(self):
        import typer
        with self.assertRaises(typer.Exit) as stop:
            self._run(self._patch([False, False]))
        self.assertEqual(stop.exception.exit_code, 1)

    def test_agent_gets_clear_message_and_exit_code_3(self):
        import io
        import typer
        from contextlib import redirect_stderr
        err = io.StringIO()
        with mock.patch.object(internal_certs, "ensure_trusted", side_effect=[False]) as ensure, \
                mock.patch.object(internal_certs, "cert_needed", return_value=(True, "On the IIASA network.")), \
                mock.patch.object(internal_certs, "can_prompt", return_value=False), \
                mock.patch.object(typer, "echo", side_effect=lambda msg, err=False: print(msg, file=sys_stderr())):
            with redirect_stderr(err), self.assertRaises(typer.Exit) as stop:
                self.cli.require_storage_cert("https://acc.example")
        self.assertEqual(stop.exception.exit_code, internal_certs.EXIT_NEEDS_PERSON)
        ensure.assert_called_once_with(install=False)  # no prompt was attempted
        message = err.getvalue()
        self.assertIn("ACTION REQUIRED", message)
        self.assertIn("accli cert install", message)
        self.assertIn("On the IIASA network.", message)

    def _invoke(self, args):
        import typer
        from typer.testing import CliRunner
        with mock.patch.object(self.cli, "require_storage_cert", side_effect=typer.Exit(7)) as check, \
                mock.patch.object(self.cli, "check_for_updates", return_value=(False, None)):
            result = CliRunner().invoke(self.cli.app, args)
        return check, result

    def test_download_checks_certificate(self):
        check, result = self._invoke(["copy", "acc://proj/file.txt", "./file.txt"])
        check.assert_called_once()
        self.assertEqual(result.exit_code, 7)

    def test_upload_does_not_check_certificate(self):
        with tempfile.NamedTemporaryFile() as f:
            check, _ = self._invoke(["copy", f.name, "acc://proj/file.txt"])
        check.assert_not_called()


class TestCertCommands(unittest.TestCase):
    def setUp(self):
        from accli import cli
        self.cli = cli

    def _status(self, trusted, network):
        from typer.testing import CliRunner
        with mock.patch.object(internal_certs, "ensure_trusted", return_value=trusted), \
                mock.patch.object(internal_certs, "storage_network", return_value=network), \
                mock.patch.object(self.cli, "get_server_url", return_value="https://acc.example"), \
                mock.patch.object(self.cli, "get_access_token", return_value="tok"), \
                mock.patch.object(self.cli, "check_for_updates", return_value=(False, None)):
            return CliRunner().invoke(self.cli.app, ["cert", "status", "--json"])

    def test_status_json_stays_clean_when_session_expired(self):
        import json
        import typer
        from typer.testing import CliRunner

        def expired():
            print("ERROR: Your session has expired or the token is invalid.")  # what the real helper prints
            raise typer.Exit(1)

        with mock.patch.object(internal_certs, "ensure_trusted", return_value=False), \
                mock.patch.object(internal_certs, "storage_network") as network, \
                mock.patch.object(self.cli, "get_server_url", return_value="https://acc.example"), \
                mock.patch.object(self.cli, "get_access_token", side_effect=expired), \
                mock.patch.object(self.cli, "check_for_updates", return_value=(False, None)):
            result = CliRunner().invoke(self.cli.app, ["cert", "status", "--json"])
        data = json.loads(result.stdout)  # only JSON on stdout
        self.assertIsNone(data["internal_network"])
        self.assertIn("accli login", data["reason"])
        network.assert_not_called()  # no request without a token

    def test_status_internal_without_certificate(self):
        import json
        result = self._status(False, {"internal": True, "storage_host": internal_certs.CERT_HOST})
        self.assertEqual(result.exit_code, internal_certs.EXIT_NEEDS_PERSON)
        data = json.loads(result.stdout)
        self.assertEqual((data["installed"], data["internal_network"], data["action_required"]), (False, True, True))
        self.assertEqual(data["fix_command"], "accli cert install")

    def test_status_external_without_certificate(self):
        import json
        result = self._status(False, {"internal": False, "storage_host": None})
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(json.loads(result.stdout)["action_required"])

    def test_status_installed(self):
        import json
        result = self._status(True, {"internal": True, "storage_host": internal_certs.CERT_HOST})
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(json.loads(result.stdout)["installed"])


def sys_stderr():
    import sys
    return sys.stderr


def make_cert(names=("hctr02obj81.iiasa.ac.at",), ca=False, basic_constraints=True,
              valid_from_days=-1, valid_for_days=365, ip=None):
    """A self-signed test certificate, built to try the safety checks."""
    import datetime
    import ipaddress
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0] if names else "test")])
    start = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=valid_from_days)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(subject).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start).not_valid_after(start + datetime.timedelta(days=valid_for_days))
    )
    entries = [x509.DNSName(n) for n in names] + ([x509.IPAddress(ipaddress.ip_address(ip))] if ip else [])
    if entries:
        builder = builder.add_extension(x509.SubjectAlternativeName(entries), critical=False)
    if basic_constraints:
        builder = builder.add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    cert = builder.sign(key, hashes.SHA256())
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


class TestCertificateSafetyChecks(unittest.TestCase):
    """A certificate from the server ends up trusted by the computer, so only a narrow one is accepted."""

    def test_shipped_and_normal_certificates_are_accepted(self):
        sha1, host, _ = internal_certs.validate_cert_pem(internal_certs.cert_path().read_text())
        self.assertEqual((sha1, host), (internal_certs.CERT_SHA1, internal_certs.CERT_HOST))
        self.assertEqual(internal_certs.validate_cert_pem(make_cert())[1], "hctr02obj81.iiasa.ac.at")

    def test_dangerous_certificates_are_refused(self):
        cases = {
            "certificate authority (could vouch for any website)": make_cert(ca=True),
            "does not say whether it is a certificate authority": make_cert(basic_constraints=False),
            "not an IIASA server": make_cert(names=("www.google.com",)),
            "lookalike domain": make_cert(names=("evil-iiasa.ac.at",)),
            "wildcard": make_cert(names=("*.iiasa.ac.at",)),
            "more than one server": make_cert(names=("a.iiasa.ac.at", "b.iiasa.ac.at")),
            "an IP address as well": make_cert(ip="147.125.1.1"),
            "no server name": make_cert(names=()),
            "expired": make_cert(valid_from_days=-400, valid_for_days=30),
            "not valid yet": make_cert(valid_from_days=10),
            "two certificates in one": make_cert() + make_cert(),
            "not a certificate": "-----BEGIN CERTIFICATE-----\nnonsense\n-----END CERTIFICATE-----\n",
        }
        for label, pem in cases.items():
            with self.assertRaises(ValueError, msg=label):
                internal_certs.validate_cert_pem(pem)


class TestCertificateFromServer(unittest.TestCase):
    """Getting the current certificate from the Accelerator server (handles a certificate change)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        patches = [
            mock.patch.object(internal_certs, "SAVED_CERTS_DIR", self.tmp / "storage-certs"),
            mock.patch.object(internal_certs, "SERVER_CHECK_FILE", self.tmp / "storage-certs" / "last-check.json"),
            mock.patch.object(internal_certs, "BUNDLE_PATH", self.tmp / "ca-bundle.pem"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(shutil.rmtree, self.tmp)

    def _server_says(self, pem):
        info = {"internal": True, "storage_host": "hctr02obj81.iiasa.ac.at", "storage_certificate": pem}
        return mock.patch.object(internal_certs, "storage_network", return_value=info)

    def test_without_server_certificate_the_shipped_one_is_used(self):
        self.assertEqual(internal_certs.current_cert().source, "bundled")
        self.assertEqual(internal_certs.current_cert().sha1, internal_certs.CERT_SHA1)

    def test_new_certificate_from_server_becomes_current(self):
        new = make_cert()
        with self._server_says(new):
            info, warning = internal_certs.refresh_from_server("https://acc.example", "tok")
        self.assertIsNone(warning)
        self.assertTrue(info["internal"])
        current = internal_certs.current_cert()
        self.assertEqual((current.source, current.host), ("server", "hctr02obj81.iiasa.ac.at"))
        self.assertEqual(current.pem, new)

    def test_refused_certificate_is_not_saved_and_explained(self):
        with self._server_says(make_cert(ca=True)):
            _, warning = internal_certs.refresh_from_server("https://acc.example", "tok")
        self.assertIn("will not install", warning)
        self.assertIn("certificate authority", warning)
        self.assertEqual(internal_certs.current_cert().source, "bundled")

    def test_linux_bundle_keeps_old_and_new_during_a_change(self):
        # While the storage server switches over, both certificates must work.
        with self._server_says(make_cert()):
            internal_certs.refresh_from_server("https://acc.example", "tok")
        with mock.patch.object(internal_certs, "_USER_SSL_CERT_FILE", None):
            text = internal_certs.apply_cert_bundle().read_text()
        self.assertIn(internal_certs.cert_path().read_text().strip(), text)  # old (shipped)
        self.assertIn(internal_certs.current_cert().pem.strip(), text)  # new (from server)

    def test_asks_server_at_most_once_a_day(self):
        self.assertTrue(internal_certs.server_check_due())
        with self._server_says(None):
            internal_certs.refresh_from_server("https://acc.example", "tok")
        self.assertFalse(internal_certs.server_check_due())
        with mock.patch.object(internal_certs.time, "time", return_value=internal_certs.time.time() + 25 * 3600):
            self.assertTrue(internal_certs.server_check_due())

    def test_unreachable_server_is_asked_again_next_time(self):
        with mock.patch.object(internal_certs, "storage_network", return_value=None):
            internal_certs.refresh_from_server("https://acc.example", "tok")
        self.assertTrue(internal_certs.server_check_due())

    def test_windows_installs_the_current_certificate(self):
        with self._server_says(make_cert()):
            internal_certs.refresh_from_server("https://acc.example", "tok")
        new_sha1 = internal_certs.current_cert().sha1
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="True" if len(calls) > 1 else "", stderr="")

        with mock.patch.object(internal_certs.subprocess, "run", side_effect=fake_run):
            self.assertTrue(internal_certs.windows_install_cert())
        self.assertIn(new_sha1, calls[0][-1])  # the copied file is named after it
        self.assertIn(new_sha1, calls[1][-1])  # and that is what is checked afterwards


if __name__ == "__main__":
    unittest.main()
