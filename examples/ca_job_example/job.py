import os
import wget
from accli import Fs, AjobCliService

input_file = os.environ.get('INPUT_FILE')
file_url = Fs.get_file_url(input_file)

print('Downloaing..')

wget(file_url, 'xyz.xyz')

print('Downloaded')

Fs.write_file('xyz.xyz', 'xyz.xyz')

