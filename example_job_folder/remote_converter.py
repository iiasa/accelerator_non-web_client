import os
import fsspec
import xarray as xr
import aiohttp
import asyncio
import ssl

import numpy
import mercantile
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import Fs, AjobCliService

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def get_client(**kwargs):
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_context),
        **kwargs
    )

print(os.environ)

input_file = os.environ.get('INPUT_FILE')

validation_template_slug = os.environ.get('VALIDATION_TEMPLATE_SLUG')

user_token = os.environ.get("ACC_JOB_TOKEN", None)
server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

project_service = AjobCliService(
    user_token,
    server_url=server_url,
    verify_cert=False
)

if not (input_file or validation_template_slug):
    raise ValueError('Env variable INPUT_FILE or VALIDATION_TEMPLATE_SLUG not set.')

file_url = Fs.get_file_url(input_file)


dataset_template_details = project_service.get_dataset_template_details(validation_template_slug)
template_rules =  dataset_template_details.get('rules')

root_schema_declarations = template_rules.get('root_schema_declarations')

variable_dim_name = root_schema_declarations.get('variable_dim_name')

fs = fsspec.filesystem('https', get_client=get_client)
with fs.open(file_url) as f:

    dataset = xr.open_dataset(f)


    # Check bounds
    time, x_dim, y_dim =  dataset.data_vars[variable_dim_name].dims


    required_bounds = root_schema_declarations.get('bounds')

    dataset_bounds = [
        dataset[x_dim].min().values.item(), 
        dataset[y_dim].min().values.item(), 
        dataset[x_dim].max().values.item(), 
        dataset[y_dim].max().values.item(),
        dataset.sizes[x_dim],
        dataset.sizes[y_dim],

    ]

    assert required_bounds == dataset_bounds, ValueError(
        f"Invalid bound: {dataset_bounds}. Required bounds: {required_bounds}"
    )
    # End check bounds

    # Extract COG GeoTIFF
    for band_number in range(1, len(dataset.data_vars[variable_dim_name][time]) + 1):
        
        img_array = numpy.random.rand(nbands, height, width).astype(numpy.float32)
        src_transform = from_bounds(*bounds, width=width, height=height)
    # End Extract COG GeoTIFF

    
    # dataset_variables = dataset.data_vars
    # dataset_dims = dataset.dims

    # print("We are printing in WKUBE :)")
    # print(f"Dataset variables: {dataset_variables}")
    # print(f"Dataset dims: {dataset_dims}")

    # print(template_rules)

    # print(file['time'])
    # print(file['rsds'][9].data)

    # bounds = (
    #     file['lon'].min().values.item(), 
    #     file['lat'].min().values.item(), 
    #     file['lon'].max().values.item(), 
    #     file['lat'].max().values.item()
    # )

    # # Rasterio uses numpy array of shape of `(bands, height, width)`
    # width = file.sizes['lon']
    # height = file.sizes['lat']
    # nbands = 1

    # img_array = tile = numpy.random.rand(nbands, height, width).astype(numpy.float32)

    # src_transform = from_bounds(*bounds, width=width, height=height)

    # src_profile = dict(
    #     driver="GTiff",
    #     dtype="float32",
    #     count=nbands,
    #     height=height,
    #     width=width,
    #     crs="epsg:4326",
    #     transform=src_transform,
    # )


#     with MemoryFile() as memfile:
#         with memfile.open(**src_profile) as mem:
#             # Populate the input file with numpy array
#             mem.write(img_array)

#             dst_profile = cog_profiles.get("deflate")
#             cog_translate(
#                 mem,
#                 "my-output-cog.tif",
#                 dst_profile,
#                 in_memory=True,
#                 quiet=True,
#             )
# Fs.write_file("my-output-cog.tif", "myoutput.tif")
# Fs.write_file("my-output-cog.tif", "myoutput.tif")