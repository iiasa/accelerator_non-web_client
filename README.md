# Accelerator python client


## User Guide

**Requirements**
* Python >=3.6

**Installation**
`pip install accli --user`

*You might receive following similar warning during installation*
```
 WARNING: The script accli.exe is installed in 'C:\Users\singhr\AppData\Roaming\Python\Python311\Scripts' which is not on PATH.
  Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.
```

*If this is the case, you can goto executable directory path and execute the executable by prefixing it with `./`. For example `c:/>./accli.exe` (Windows) or `user@1khh:~#./accli` (linux)*

*You could also add executable directory path in PATH environment variable. Please follow following links for instruction on adding executable directory path to PATH environemnt variable.*

[Updating PATH on windows](https://stackoverflow.com/questions/44272416/how-to-add-a-folder-to-path-environment-variable-in-windows-10-with-screensho)
[Updating PATH on linux](https://www.geeksforgeeks.org/how-to-set-path-permanantly-in-linux/)



## Developer Guide
**Build**
Please follow [this link.](https://packaging.python.org/en/latest/tutorials/packaging-projects/)

**Tagging**
`git tag $(grep -E -i -o "(\"|')(.*?)(\"|')" accli/_version.py)`
