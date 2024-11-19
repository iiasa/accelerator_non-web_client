import sys
import math
import wget
import os
import requests
import rasterio
import tempfile
import fsspec
import xarray as xr
import rioxarray
import aiohttp
import asyncio
import ssl
from osgeo import gdal

import numpy as np
import mercantile
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import Fs, AjobCliService


dataset = xr.open_dataset(
        'FN.tif', 
        engine="rasterio", 
        mask_and_scale=False
    )

variables = dataset.data_vars

array = dataset['band_data'].values



# Get the shape of the array
# shape = array.shape

# for i in range(0, 37778):
#     print(np.unique(array[0][i]))


import pdb
pdb.set_trace()
print(shape)