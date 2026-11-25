"""Trust for IIASA's internal object storage certificate.

On the IIASA network, data is transferred directly to the object storage server
hctr01obj81.iiasa.ac.at, which uses a self-signed certificate. accli makes sure
this certificate is trusted before it transfers data:

- Windows: imported into the machine-wide Root store (needs one administrator
  prompt). Machine-wide, because the mount runs as the SYSTEM account. Python,
  hf_xet and hf-mount all read this store.
- macOS: added to the System keychain (needs one password prompt), which hf_xet
  and hf-mount read.
- Linux: accli writes ~/.accli/ca-bundle.pem (the system's trusted certificates
  plus this one) and points SSL_CERT_FILE at it. hf_xet and hf-mount read
  SSL_CERT_FILE. No administrator rights needed.
"""

import hashlib
import os
import shlex
import shutil
import ssl
import subprocess
import sys
import tempfile
from pathlib import Path

CERT_FILENAME = "iiasa-internal-s3.crt"
CERT_HOST = "hctr01obj81.iiasa.ac.at"
# SHA-1 fingerprint of the certificate, as Windows shows it (its "thumbprint").
# setup-nfs.ps1 has the same value; tests/test_internal_certs.py checks they match.
CERT_SHA1 = "11E46565A6A204870D0DDFD258863CAF8459F87D"

# Where Linux and macOS keep the system's trusted certificates.
SYSTEM_BUNDLES = [
    "/etc/ssl/certs/ca-certificates.crt",  # Debian, Ubuntu, Arch, Alpine
    "/etc/pki/tls/certs/ca-bundle.crt",  # Fedora, RHEL, CentOS
    "/etc/ssl/ca-bundle.pem",  # openSUSE
    "/etc/pki/tls/cacert.pem",  # OpenELEC
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  # RHEL 7+
    "/etc/ssl/cert.pem",  # macOS, Alpine, some others
]


def _bundle_path() -> Path:
    # Use the home folder of the account actually running accli. Under sudo, HOME may still
    # point at the invoking user's folder, and root should not write files there.
    try:
        import pwd
        home = Path(pwd.getpwuid(os.geteuid()).pw_dir)
    except Exception:
        home = Path.home()
    return home / ".accli" / "ca-bundle.pem"


BUNDLE_PATH = _bundle_path()


def cert_path() -> Path:
    """Return the bundled certificate's path, after checking it is the expected one."""
    candidates = [Path(__file__).parent / CERT_FILENAME]
    if hasattr(sys, "_MEIPASS"):
        candidates += [Path(sys._MEIPASS) / "accli" / CERT_FILENAME, Path(sys._MEIPASS) / CERT_FILENAME]

    for path in candidates:
        if path.is_file():
            if fingerprint(path) != CERT_SHA1:
                raise RuntimeError(f"The certificate at {path} is not the expected IIASA storage certificate.")
            return path

    raise RuntimeError(f"The IIASA storage certificate '{CERT_FILENAME}' is missing from the accli package.")


def fingerprint(path: Path) -> str:
    der = ssl.PEM_cert_to_DER_cert(Path(path).read_text(encoding="ascii"))
    return hashlib.sha1(der).hexdigest().upper()


# ---------------------------------------------------------------- Certificate bundle (Linux, macOS)


BUNDLE_MARKER = "\n\n# IIASA internal object storage (" + CERT_HOST + ")\n"

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
    """Write the certificate bundle and point SSL_CERT_FILE at it for this process and its children."""
    base = _system_bundle().read_text(encoding="utf-8", errors="ignore")
    # If the base is a copy of an earlier accli bundle, drop the part accli added.
    content = base.split(BUNDLE_MARKER)[0].rstrip("\n")
    content += BUNDLE_MARKER
    content += cert_path().read_text(encoding="ascii")

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


def windows_cert_installed() -> bool:
    """True if the certificate is in the machine-wide Root store (visible to SYSTEM and every user)."""
    try:
        return _powershell(f"Test-Path 'Cert:\\LocalMachine\\Root\\{CERT_SHA1}'").stdout.strip().lower() == "true"
    except Exception:
        return False


def windows_install_cert() -> bool:
    """Import the certificate into the machine-wide Root store. Windows shows an administrator (UAC) prompt."""
    temp_cert = Path(tempfile.gettempdir()) / CERT_FILENAME
    shutil.copyfile(cert_path(), temp_cert)
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
    return windows_cert_installed()


# ---------------------------------------------------------------- macOS


def macos_cert_trusted() -> bool:
    try:
        res = subprocess.run(
            ["security", "verify-cert", "-c", str(cert_path()), "-p", "ssl", "-s", CERT_HOST, "-L", "-q"],
            capture_output=True,
        )
        return res.returncode == 0
    except Exception:
        return False


def macos_install_cert() -> bool:
    """Add the certificate to the System keychain.

    Uses the macOS administrator password dialog, which works from a terminal and from
    the GUI (sudo cannot ask for a password without a terminal).
    """
    add_cmd = ["security", "add-trusted-cert", "-d", "-r", "trustRoot",
               "-k", "/Library/Keychains/System.keychain", str(cert_path())]
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
    return macos_cert_trusted()


# ---------------------------------------------------------------- All systems


def ensure_trusted(install: bool = True) -> bool:
    """Make sure the certificate is trusted on this computer, installing it if needed.

    Returns True when it is trusted. Raises RuntimeError if the bundled certificate is missing or wrong.
    """
    if sys.platform.startswith("linux"):
        apply_cert_bundle()
        return True

    if sys.platform == "darwin":
        return macos_cert_trusted() or (install and macos_install_cert())

    if sys.platform == "win32":
        return windows_cert_installed() or (install and windows_install_cert())

    return True
