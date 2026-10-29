import os
import shutil
import wget
import ssl
from accli import Fs, AjobCliService

ssl._create_default_https_context = ssl._create_unverified_context

input_file1 = 'forest-navigator/TempDemo/SpatialData/ScenarioA/VariableX/2010.tif'
input_file2 = 'forest-navigator/TempDemo/SpatialData/ScenarioA/VariableX/2020.tif'

file_url1 = Fs.get_file_url(input_file1)

print('Downloaing1..')

wget.download(file_url1, 'xyz1.xyz')

print('Downloaded1')

file_url2 = Fs.get_file_url(input_file2)

print('Downloaing2..')

wget.download(file_url2, 'xyz2.xyz')

print('Downloaded2')

# Outputs written under /mnt/wdrv are uploaded by the job agent.
for source, dest in [('xyz1.xyz', '1/xyz.xyz'), ('xyz2.xyz', '2/xyz.xyz')]:
    dest_path = os.path.join('/mnt/wdrv', dest)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copy(source, dest_path)

