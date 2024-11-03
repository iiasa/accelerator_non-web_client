import os
import wget
import ssl
from accli import Fs, AjobCliService

ssl._create_default_https_context = ssl._create_unverified_context

input_file = os.environ.get('INPUT_FILE')
file_url = Fs.get_file_url(input_file)

print('Downloaing..')

wget.download(file_url, 'xyz.xyz')

print('Downloaded')

Fs.write_file('xyz.xyz', 'xyz.xyz')

