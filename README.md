# Accelerator Terminal Client and Python API

This package provides:

- A **command-line client** (`accli`) for interacting with the **Accelerator**.
- A **Python API** via the `accli` package.

Both interfaces communicate with the Accelerator’s REST API, implemented in
the [Control Services Backend](https://github.com/iiasa/control_services_backend).

---

## 🔐 Authentication

This client uses **device authentication** via Auth0.

- **OAuth Flow
  **: [Device Authorization Flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow)
- **Token Validity**: 7 days
- **Access Control**: Role-Based Access Control (RBAC) with stateless tokens
- **Grants**: Limited and scoped

---

## 📖 User Guide

### ✅ Requirements

- Python >= 3.8

### 📦 Installation

#### Option 1: Standalone Binaries (Recommended)

No Python install required. Run the appropriate command for your system:

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/iiasa/accli/master/scripts/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/iiasa/accli/master/scripts/install.ps1 | iex
```

#### Option 2: Via PIP

```bash
pip install accli --user
```

#### IIASA storage certificate

On the IIASA network, downloads go directly to the internal storage server (currently `hctr01obj81.iiasa.ac.at`), which uses a self-signed certificate. Uploads always go through the Accelerator server and never need it.

- **Downloads (`accli copy acc://… <local path>`):** the Accelerator server tells accli whether your computer counts as internal (VPN connections do not). Outside the IIASA network nothing is required.
- **Mounts (`accli mount start`):** the certificate is always required, on any network. A mount keeps running when a laptop moves onto the IIASA network, and its reads would then fail without the certificate.

accli ships the certificate and makes sure it is trusted:

- **Windows:** installed into the machine-wide trusted certificates. Windows asks for administrator approval once.
- **macOS:** added to the System keychain. macOS asks for your password once.
- **Linux:** accli writes `~/.accli/ca-bundle.pem` (the system's trusted certificates plus this one) and uses it automatically. No administrator rights are needed.

To check or install it yourself:

```bash
accli cert status          # add --json for a machine-readable result
accli cert install
```

IT administrators can install it ahead of time with `setup-nfs.ps1` on Windows (see [SECURITY.md](SECURITY.md)).

**When the storage certificate changes:** accli asks the Accelerator server for the current certificate at most once a day (and every time you run `accli cert status` or `accli cert install`), saves it in `~/.accli/storage-certs/`, and asks you to install it, the same way as the first time. No accli update is needed. Certificates installed earlier stay trusted, so nothing breaks during the switch.

Because a certificate from the network ends up trusted by your computer, accli only accepts one that cannot vouch for anything else: not a certificate authority, valid for exactly one server under `iiasa.ac.at` (no wildcards), and currently valid. Anything else is refused with a warning, and accli keeps using the certificate it has.

For server administrators: the Accelerator server publishes the current certificate by default (`configs/InternalStorageCert.py`). To change it, replace it there, or set `XET_CAS_S3_INTERNAL_CERT_BASE64` (the base64 of the PEM file), **at least a day before** switching the storage server to it, so users have it before it is needed.

#### AI agents and scripts

When accli runs without an interactive terminal (AI agents, scripts, CI), it never opens an administrator prompt, because nobody could answer it. If a person has to act first, the command stops with **exit code 3** and prints a message starting with `ACTION REQUIRED:` that says what to ask the user to run (for example `accli cert install`). Exit code 1 still means other errors.

- `accli cert status --json` lets an agent check in advance: `"action_required": true` means a person must run `"fix_command"`.
- `ACCLI_NO_PROMPT=1` turns prompts off even in a terminal. Set it for agent tools that run commands in a terminal, so accli stops with exit code 3 instead of waiting on a dialog nobody sees.
- The same guidance is printed by `accli --help`, and in short by `accli copy --help`, `accli mount start --help` and `accli cert --help`.

### ▶️ Usage

#### As a Python Module

```bash
python -m accli
```

#### As an Executable

After installation, the executable might not be in your system `PATH`. You may see a warning like this:

```
WARNING: The script accli.exe is installed in 'C:\Users\<user>\AppData\Roaming\Python\Python311\Scripts' which is not on PATH.
Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
```

To resolve this, add the script's path to your system's environment variables:

- [Update PATH on Windows](https://stackoverflow.com/questions/44272416/how-to-add-a-folder-to-path-environment-variable-in-windows-10-with-screensho)
- [Update PATH on Linux](https://www.geeksforgeeks.org/how-to-set-path-permanantly-in-linux/)

> **Note:** On Linux/macOS, you may need to prefix the command with `./`, and on Windows, with `.\`.

### 🔍 Help Command

```bash
accli --help
```

**Sample Output:**

```
Usage: accli [OPTIONS] COMMAND [ARGS]...
```

### 📋 Jobs

List root jobs, newest first, with each job's type (`standalone`, `children`, `callback` or `children+callback`), its own status and the status of its whole graph:

```bash
accli jobs my-project                       # latest 10
accli jobs my-project -n 50
accli jobs my-project -s failed             # graphs with a failed job
accli jobs my-project -s processing,error
```

`--status` filters on the graph status: `processing` (or `active`), `error` (or `failed`) and `done`. A standalone job counts by its own status.

Show a job's status and details. For a job holding children or callbacks, Graph status covers the jobs below it (the same as GRAPH in `accli jobs`) and each one is listed; any job also links to the pipeline it is part of. Read a job's output with `accli logs <project> <job>`:

```bash
accli job my-project 1234
accli job my-project 1234 --json
accli job show my-project 1234
```

`accli job <project> <job>` is the same as `accli job show <project> <job>`.

Restart a job that is done or failed. The job can be a root job or a child job. The command fails if that job, a parent, or a descendant is still processing. `--rebuild` builds the container image again before the job starts.

```bash
accli job restart my-project 1234
accli job restart my-project 1234 --rebuild
```

---

## 👩‍💻 Developer Guide

### Handling self-signed certificates

Import the certificates from `accelerator_service` into the Windows Root store (auto-elevates to Administrator if needed):
```powershell
cd <path to>/accelerator_service/scripts
.\import_certs_certutil.ps1
```

### 🛠 Build & Upload

Follow the official Python packaging tutorial:  
[Packaging Projects – Python.org](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

### 🚀 Release Process

1. Update the version in `accli/_version.py`
2. Tag the release:

    ```bash
    python scripts/tag.py
    ```

3. Build the package:

    ```bash
    python -m build
    ```

4. Upload to PyPI:

    ```bash
    twine upload -r pypi -u __token__ -p <password-or-token> ./dist/*
    ```

---