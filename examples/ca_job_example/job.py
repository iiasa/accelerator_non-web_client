import os
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

Fs.write_file('xyz1.xyz', '1/xyz.xyz')
Fs.write_file('xyz2.xyz', '2/xyz.xyz')

