
# Accelerator Terminal Client and Python API

This package provides:

- A **command-line client** (`accli`) for interacting with the **Accelerator**.
- A **Python API** via the `accli` package.

Both interfaces communicate with the Accelerator’s REST API, implemented in the [Control Services Backend](https://github.com/iiasa/control_services_backend).

---

## 🔐 Authentication

This client uses **device authentication** via Auth0.

- **OAuth Flow**: [Device Authorization Flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow)  
- **Token Validity**: 7 days  
- **Access Control**: Role-Based Access Control (RBAC) with stateless tokens  
- **Grants**: Limited and scoped  

---

## 📖 User Guide

### ✅ Requirements

- Python >= 3.7.17

### 📦 Installation

```bash
pip install accli --user
```

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

---

## 👩‍💻 Developer Guide

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