import os
import fsspec
import xarray as xr

import numpy
import mercantile
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import Fs



input_file = os.environ.get('INPUT_FILE')

validation_template_slug = os.environ.get('VALIDATION_TEMPLATE_SLUG')

if not (input_file or validation_template_slug):
    raise ValueError('Env variable INPUT_FILE or VALIDATION_TEMPLATE_SLUG not set.')

file_url = Fs.get_file_url(input_file)

of = fsspec.open(input_file)

with of as f:

    dataset = xr.open_dataset(f)
    
    dataset_variables = dataset.data_vars
    dataset_dims = dataset.dims

    print("We are printing in WKUBE :)")
    print(f"Dataset variables {dataset_variables}")
    print(f"Dataset dims {dataset_dims}")

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