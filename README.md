# Accelerator terminal client and Python API

Provides a command-line client for interacting with the Accelerator as well as a Python API in the form of the `accli` package. These communicate with the rest API of the Accelerator [Control Services Backend](https://github.com/iiasa/control_services_backend).

Uses device authentication:
- Part 0Auth: [Device Authentication Flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/device-authorization-flow).
- Auth valid for 7 days.
- Grants are limited.
- Access via RBAC:
  * Stateless tokens.

## User Guide

**Requirements**
* Python >=3.7.17

**Installation**

`pip install accli --user`

**Usage as module**

`python -m accli`

**Usage as executable**

*You might receive following similar warning during installation*
```
 WARNING: The script accli.exe is installed in 'C:\Users\singhr\AppData\Roaming\Python\Python311\Scripts' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
```

*You could also add executable directory path in PATH environment variable. Please follow following links for instruction on adding executable directory path to PATH environemnt variable.*

[Updating PATH on windows](https://stackoverflow.com/questions/44272416/how-to-add-a-folder-to-path-environment-variable-in-windows-10-with-screensho)

[Updating PATH on linux](https://www.geeksforgeeks.org/how-to-set-path-permanantly-in-linux/)

**Command**

`accli --help`

*Output*

`Usage: accli [OPTIONS] COMMAND [ARGS]...`

*Note: You may need to prepend the command with either `./`(in linux) or `.\`(in winodws).*


## Developer Guide
**General build and upload instructions**
Please follow [this link.](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

**Release process**
1. Commit with right version on accli/_version.py
2. Run 'python scripts/tag.py'
3. `python -m build`
4. `twine upload -r pypi -u __token__ -p <password-or-token> ./dist/*`
