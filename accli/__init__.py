import urllib3
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

from accli._version import VERSION

from accli.cli import app

from accli.AcceleratorTaskDispatcher import *

from accli.AcceleratorJobProjectService import AcceleratorJobProjectService, Fs

AjobCliService = AcceleratorJobProjectService
