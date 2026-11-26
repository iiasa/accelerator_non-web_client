"""Trust for IIASA's internal object storage certificate.

On the IIASA network, data is transferred directly to the internal object storage
server, which uses a self-signed certificate. accli makes sure this certificate is
trusted before it transfers data:

- Windows: imported into the machine-wide Root store (needs one administrator
  prompt). Machine-wide, because the mount runs as the SYSTEM account. Python,
  hf_xet and hf-mount all read this store.
- macOS: added to the System keychain (needs one password prompt), which hf_xet
  and hf-mount read.
- Linux: accli writes ~/.accli/ca-bundle.pem (the system's trusted certificates
  plus the storage certificates) and points SSL_CERT_FILE at it. hf_xet and
  hf-mount read SSL_CERT_FILE. No administrator rights needed.

Where the certificate comes from: accli ships one (iiasa-internal-s3.crt), and the
Accelerator server publishes the current one at /api/xet-cas/v1/network. accli asks
at most once a day and saves what it gets in ~/.accli/storage-certs/, so a new
storage certificate reaches users without an accli release. Because a certificate
from the network ends up trusted by the operating system, accli only accepts one
that can vouch for a single IIASA server and nothing else (see validate_cert_pem).
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import shlex
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

CERT_FILENAME = "iiasa-internal-s3.crt"
CERT_HOST = "hctr01obj81.iiasa.ac.at"  # host of the certificate shipped with accli
# SHA-1 fingerprint of the shipped certificate, as Windows shows it (its "thumbprint").
# setup-nfs.ps1 has the same value as its default; tests/test_internal_certs.py checks they match.
CERT_SHA1 = "11E46565A6A204870D0DDFD258863CAF8459F87D"

# A storage certificate may only name a single server under this domain.
ALLOWED_DOMAIN = ".iiasa.ac.at"
# How often accli asks the server for the current certificate.
SERVER_CHECK_INTERVAL = 24 * 60 * 60

# Where Linux and macOS keep the system's trusted certificates.
SYSTEM_BUNDLES = [
    "/etc/ssl/certs/ca-certificates.crt",  # Debian, Ubuntu, Arch, Alpine
    "/etc/pki/tls/certs/ca-bundle.crt",  # Fedora, RHEL, CentOS
    "/etc/ssl/ca-bundle.pem",  # openSUSE
    "/etc/pki/tls/cacert.pem",  # OpenELEC
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  # RHEL 7+
    "/etc/ssl/cert.pem",  # macOS, Alpine, some others
]


def _accli_dir() -> Path:
    # Use the home folder of the account actually running accli. Under sudo, HOME may still
    # point at the invoking user's folder, and root should not write files there.
    try:
        import pwd
        home = Path(pwd.getpwuid(os.geteuid()).pw_dir)
    except Exception:
        home = Path.home()
    return home / ".accli"


BUNDLE_PATH = _accli_dir() / "ca-bundle.pem"
SAVED_CERTS_DIR = _accli_dir() / "storage-certs"
SERVER_CHECK_FILE = SAVED_CERTS_DIR / "last-check.json"


# ---------------------------------------------------------------- The certificate


@dataclass
class StorageCert:
    path: Path  # PEM file on disk
    sha1: str  # fingerprint (Windows "thumbprint")
    host: str  # the one server name it is valid for
    source: str  # "bundled" or "server"
    not_after: datetime.datetime

    @property
    def pem(self) -> str:
        return self.path.read_text(encoding="ascii")


def _utc(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)


def validate_cert_pem(pem: str) -> tuple[str, str, datetime.datetime]:
    """Check that a certificate is safe to trust, and return (sha1, host, not_after).

    Trusting a certificate tells the operating system to accept it for every name it
    lists, and, if it is a certificate authority, for everything it signs. So accli only
    accepts exactly one certificate that is not a certificate authority, names exactly one
    server under iiasa.ac.at (no wildcards, no IP addresses), and is currently valid.
    Raises ValueError otherwise.
    """
    from cryptography import x509

    if pem.count("-----BEGIN CERTIFICATE-----") != 1:
        raise ValueError("expected exactly one certificate")
    try:
        cert = x509.load_pem_x509_certificate(pem.encode("ascii"))
    except Exception as e:
        raise ValueError(f"not a readable certificate ({e})")

    try:
        basic = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
    except x509.ExtensionNotFound:
        raise ValueError("it does not state that it is not a certificate authority")
    if basic.ca:
        raise ValueError("it is a certificate authority, which could vouch for any website")

    try:
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        raise ValueError("it does not name the server it is for")
    names = [entry for entry in san]
    dns_names = san.get_values_for_type(x509.DNSName)
    if len(names) != 1 or len(dns_names) != 1:
        raise ValueError("it must name exactly one server")
    host = dns_names[0].lower()
    if "*" in host or not host.endswith(ALLOWED_DOMAIN):
        raise ValueError(f"it is for '{host}', not a single server under {ALLOWED_DOMAIN.lstrip('.')}")

    now = datetime.datetime.now(datetime.timezone.utc)
    not_before = _utc(getattr(cert, "not_valid_before_utc", None) or cert.not_valid_before)
    not_after = _utc(getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after)
    if not (not_before <= now <= not_after):
        raise ValueError(f"it is not valid now (valid {not_before:%Y-%m-%d} to {not_after:%Y-%m-%d})")

    der = ssl.PEM_cert_to_DER_cert(pem)
    return hashlib.sha1(der).hexdigest().upper(), host, not_after


def fingerprint(path: Path) -> str:
    der = ssl.PEM_cert_to_DER_cert(Path(path).read_text(encoding="ascii"))
    return hashlib.sha1(der).hexdigest().upper()


def bundled_cert() -> StorageCert:
    """The certificate shipped with accli, after checking it is the expected one."""
    candidates = [Path(__file__).parent / CERT_FILENAME]
    if hasattr(sys, "_MEIPASS"):
        candidates += [Path(sys._MEIPASS) / "accli" / CERT_FILENAME, Path(sys._MEIPASS) / CERT_FILENAME]

    for path in candidates:
        if path.is_file():
            sha1, host, not_after = validate_cert_pem(path.read_text(encoding="ascii"))
            if sha1 != CERT_SHA1:
                raise RuntimeError(f"The certificate at {path} is not the expected IIASA storage certificate.")
            return StorageCert(path, sha1, host, "bundled", not_after)

    raise RuntimeError(f"The IIASA storage certificate '{CERT_FILENAME}' is missing from the accli package.")


def cert_path() -> Path:
    """Path of the bundled certificate (kept for callers that only need the file)."""
    return bundled_cert().path


def saved_certs() -> list[StorageCert]:
    """Certificates received from the server, newest first. Invalid or expired ones are skipped."""
    found = []
    if SAVED_CERTS_DIR.is_dir():
        for path in SAVED_CERTS_DIR.glob("*.pem"):
            try:
                sha1, host, not_after = validate_cert_pem(path.read_text(encoding="ascii"))
            except (ValueError, OSError):
                continue
            found.append((path.stat().st_mtime, StorageCert(path, sha1, host, "server", not_after)))
    return [cert for _, cert in sorted(found, key=lambda item: item[0], reverse=True)]


def current_cert() -> StorageCert:
    """The certificate the storage server uses now: the latest from the server, else the bundled one."""
    saved = saved_certs()
    return saved[0] if saved else bundled_cert()


def all_known_certs() -> list[StorageCert]:
    """Every storage certificate accli knows (saved and bundled), without duplicates."""
    certs, seen = [], set()
    for cert in [*saved_certs(), bundled_cert()]:
        if cert.sha1 not in seen:
            seen.add(cert.sha1)
            certs.append(cert)
    return certs


# ---------------------------------------------------------------- Getting it from the server


def server_check_due() -> bool:
    try:
        last = json.loads(SERVER_CHECK_FILE.read_text())["checked_at"]
        return time.time() - float(last) > SERVER_CHECK_INTERVAL
    except Exception:
        return True


def save_server_cert(pem: str) -> StorageCert:
    """Validate a certificate from the server and save it. Raises ValueError if it is refused."""
    sha1, host, not_after = validate_cert_pem(pem)
    SAVED_CERTS_DIR.mkdir(parents=True, exist_ok=True)
    path = SAVED_CERTS_DIR / f"{sha1}.pem"
    if not path.is_file() or path.read_text(encoding="ascii") != pem:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(pem, encoding="ascii")
        os.replace(tmp, path)
    os.utime(path)  # newest = current
    return StorageCert(path, sha1, host, "server", not_after)


def refresh_from_server(server_url: str, access_token: str) -> tuple[dict | None, str | None]:
    """Ask the server about the network and the current certificate, and save the certificate.

    Returns (network info or None, warning or None). The warning explains a refused certificate.
    """
    info = storage_network(server_url, access_token)
    if info is None:
        return None, None
    warning = None
    pem = info.get("storage_certificate")
    if pem:
        try:
            save_server_cert(pem)
        except ValueError as e:
            warning = f"The Accelerator server offered a storage certificate that accli will not install: {e}."
    try:
        SAVED_CERTS_DIR.mkdir(parents=True, exist_ok=True)
        SERVER_CHECK_FILE.write_text(json.dumps({"checked_at": time.time()}))
    except OSError:
        pass
    return info, warning


# ---------------------------------------------------------------- Certificate bundle (Linux)


BUNDLE_MARKER = "\n\n# IIASA internal object storage\n"

# The user's own SSL_CERT_FILE, remembered before accli points it at the bundle.
_USER_SSL_CERT_FILE = os.environ.get("SSL_CERT_FILE")


def _system_bundle() -> Path:
    # A bundle the user chose themselves comes first. Our own bundle is never used as the
    # base: once SSL_CERT_FILE points at it, Python's default path does too.
    candidates = [_USER_SSL_CERT_FILE, ssl.get_default_verify_paths().cafile, *SYSTEM_BUNDLES]
    for candidate in candidates:
        if not candidate or not Path(candidate).is_file():
            continue
        try:
            if Path(candidate).resolve() == BUNDLE_PATH.resolve():
                continue
        except OSError:
            pass
        return Path(candidate)

    raise RuntimeError("Could not find the system's list of trusted certificates (for example /etc/ssl/certs/ca-certificates.crt).")


def apply_cert_bundle() -> Path:
    """Write the certificate bundle and point SSL_CERT_FILE at it for this process and its children.

    The bundle has every known storage certificate, so a mount keeps working while the
    storage server switches from an old certificate to a new one.
    """
    base = _system_bundle().read_text(encoding="utf-8", errors="ignore")
    # If the base is a copy of an earlier accli bundle, drop the part accli added.
    content = base.split(BUNDLE_MARKER)[0].rstrip("\n")
    content += BUNDLE_MARKER
    content += "\n".join(f"# {cert.host} ({cert.source})\n{cert.pem.strip()}" for cert in all_known_certs()) + "\n"

    existing = BUNDLE_PATH.read_text(encoding="utf-8", errors="ignore") if BUNDLE_PATH.is_file() else None
    if existing != content:
        BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = BUNDLE_PATH.with_suffix(".pem.tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, BUNDLE_PATH)

    os.environ["SSL_CERT_FILE"] = str(BUNDLE_PATH)
    return BUNDLE_PATH


# ---------------------------------------------------------------- Windows


def _powershell(command: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command],
        capture_output=True, text=True, errors="replace",
    )


def windows_cert_installed(cert: StorageCert | None = None) -> bool:
    """True if the certificate is in the machine-wide Root store (visible to SYSTEM and every user)."""
    sha1 = (cert or current_cert()).sha1
    try:
        return _powershell(f"Test-Path 'Cert:\\LocalMachine\\Root\\{sha1}'").stdout.strip().lower() == "true"
    except Exception:
        return False


def windows_install_cert(cert: StorageCert | None = None) -> bool:
    """Import the certificate into the machine-wide Root store. Windows shows an administrator (UAC) prompt."""
    cert = cert or current_cert()
    temp_cert = Path(tempfile.gettempdir()) / f"accli-storage-{cert.sha1}.crt"
    shutil.copyfile(cert.path, temp_cert)
    try:
        # Quoted twice: once for the elevated PowerShell, once for the outer -ArgumentList string.
        quoted_path = str(temp_cert).replace("'", "''")
        inner = (
            f"Import-Certificate -FilePath '{quoted_path}' -CertStoreLocation Cert:\\LocalMachine\\Root | Out-Null"
        ).replace("'", "''")
        _powershell(
            "Start-Process powershell.exe -Verb RunAs -Wait -WindowStyle Hidden "
            f"-ArgumentList '-NoProfile -ExecutionPolicy Bypass -Command \"{inner}\"'"
        )
    finally:
        try:
            temp_cert.unlink()
        except OSError:
            pass
    # Start-Process does not pass the elevated exit code back, so check the result directly.
    return windows_cert_installed(cert)


# ---------------------------------------------------------------- macOS


def macos_cert_trusted(cert: StorageCert | None = None) -> bool:
    cert = cert or current_cert()
    try:
        res = subprocess.run(
            ["security", "verify-cert", "-c", str(cert.path), "-p", "ssl", "-s", cert.host, "-L", "-q"],
            capture_output=True,
        )
        return res.returncode == 0
    except Exception:
        return False


def macos_install_cert(cert: StorageCert | None = None) -> bool:
    """Add the certificate to the System keychain.

    Uses the macOS administrator password dialog, which works from a terminal and from
    the GUI (sudo cannot ask for a password without a terminal).
    """
    cert = cert or current_cert()
    add_cmd = ["security", "add-trusted-cert", "-d", "-r", "trustRoot",
               "-k", "/Library/Keychains/System.keychain", str(cert.path)]
    if os.geteuid() == 0:
        subprocess.run(add_cmd)
    else:
        subprocess.run([
            "osascript",
            "-e", "on run argv",
            "-e", "do shell script (item 1 of argv) with administrator privileges",
            "-e", "end run",
            " ".join(shlex.quote(c) for c in add_cmd),
        ])
    return macos_cert_trusted(cert)


# ---------------------------------------------------------------- Who needs it, and can we ask?

# Exit code when a person has to act (install the certificate, approve a one-time setup).
# Lets scripts and AI agents tell this apart from other errors (exit code 1).
EXIT_NEEDS_PERSON = 3


def can_prompt() -> bool:
    """True if a person can answer an administrator prompt for this command.

    AI agents, scripts and CI run accli without an interactive terminal; a dialog would
    either never be seen or block the command. The GUI also has no terminal, but a
    person is sitting at it, so it sets ACCLI_GUI=1. ACCLI_NO_PROMPT=1 turns prompts off.
    """
    if os.environ.get("ACCLI_NO_PROMPT"):
        return False
    if os.environ.get("ACCLI_GUI"):
        return True
    try:
        return sys.stdin is not None and sys.stdin.isatty()
    except Exception:
        return False


def storage_network(server_url: str, access_token: str, timeout: float = 5) -> dict | None:
    """Ask the Accelerator server whether it treats this computer as internal.

    The endpoint requires a signed-in user, so this sends the user's access token.
    Returns {"internal": bool, "storage_host": str | None, "storage_certificate": str | None},
    or None if the server could not be asked (offline, rejected the token, or a server
    version without this endpoint).
    """
    import requests
    try:
        resp = requests.get(
            f"{server_url.rstrip('/')}/api/xet-cas/v1/network",
            headers={"x-authorization": access_token},
            timeout=timeout,
            verify=not os.environ.get("ACCLI_DEBUG"),
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
    except Exception:
        return None
    if not isinstance(data, dict) or not isinstance(data.get("internal"), bool):
        return None
    return data


_ASK_SERVER = object()


def cert_needed(server_url: str, access_token: str, info=_ASK_SERVER) -> tuple[bool, str]:
    """Whether this computer needs the certificate for downloads, and why (one sentence).

    Pass `info` from storage_network() (even None) to avoid asking the server again.
    """
    if info is _ASK_SERVER:
        info = storage_network(server_url, access_token)
    host = current_cert().host
    if info is None:
        return True, "accli could not check whether this computer is on the IIASA network, so it assumes it is."
    if not info["internal"]:
        return False, "This computer is not on the IIASA network, so downloads do not use the internal storage server."
    storage_host = info.get("storage_host")
    if storage_host and storage_host != host:
        return False, f"The internal storage server ({storage_host}) does not use this certificate."
    return True, f"This computer is on the IIASA network, so downloads go directly to {host}."


# ---------------------------------------------------------------- All systems


def ensure_trusted(install: bool = True) -> bool:
    """Make sure the current storage certificate is trusted on this computer, installing it if needed.

    Returns True when it is trusted. Raises RuntimeError if the bundled certificate is missing or wrong.
    """
    if sys.platform.startswith("linux"):
        apply_cert_bundle()
        return True

    cert = current_cert()

    if sys.platform == "darwin":
        return macos_cert_trusted(cert) or (install and macos_install_cert(cert))

    if sys.platform == "win32":
        return windows_cert_installed(cert) or (install and windows_install_cert(cert))

    return True
