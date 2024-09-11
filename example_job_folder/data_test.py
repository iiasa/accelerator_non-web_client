import wget
import os
import requests
import rasterio
import tempfile
import fsspec
import xarray as xr
import aiohttp
import asyncio
import ssl
from osgeo import gdal

import numpy
import mercantile
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import Fs, AjobCliService

# import rpy2.robjects as robjects
# from rpy2.robjects.packages import importr

# Load the terra package
# terra = importr('terra')

ssl._create_default_https_context = ssl._create_unverified_context

file_url = 'https://localip:9000/accelerator/brightspace/SpatialX/X.nc'

with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file_path = f"{temp_file.name}.nc"

    print(temp_file_path)

    print("Downloading..")
    wget.download(file_url, temp_file_path)
    print("Downloaded")

    dataset = xr.open_dataset(temp_file_path)

    import pdb
    pdb.set_trace

    with rasterio.open(temp_file_path) as src:
        
        nodata_value = src.nodata
        crs = src.crs

        if not nodata_value:
            nodata_value = float('nan')

        if not crs:
            pass

    print("Hello World")


    # Check bounds
    # print(dataset.data_vars)
    # variable_dim_name = 'Natural_forests'
    # time, x_dim, y_dim =  dataset.data_vars[variable_dim_name].dims

    # dataset_bounds = [
    #     dataset[x_dim].min().values.item(), 
    #     dataset[y_dim].min().values.item(), 
    #     dataset[x_dim].max().values.item(), 
    #     dataset[y_dim].max().values.item(),
    #     dataset.sizes[x_dim],
    #     dataset.sizes[y_dim],
    # ]



    # import pdb
    # pdb.set_trace()


    # # End check bounds

    # # Extract COG GeoTIFF
    # for band_number in range(1, len(dataset.data_vars[variable_dim_name][time]) + 1):
    #     print(band_number)
        
    #     print(nodata_value)
    #     print(crs)
    #     print(dataset_bounds)

    #     ds = gdal.Open(temp_file_path)

    #     prj = ds.GetProjection()
    #     print(f"Prj: {type(prj)}")
    
