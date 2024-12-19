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


ssl._create_default_https_context = ssl._create_unverified_context


input_file = os.environ.get('INPUT_FILE')

user_token = os.environ.get("ACC_JOB_TOKEN", None)
server_url = os.environ.get("ACC_JOB_GATEWAY_SERVER", None)

project_service = AjobCliService(
    user_token,
    server_url=server_url,
    verify_cert=False
)

if not input_file:
    raise ValueError('Env variable INPUT_FILE not set.')

file_url = Fs.get_file_url(input_file)

def get_crs(dataset, variable):

    crs = dataset.rio.crs

    if crs:
        return crs
    
    dims = dataset[variable].dims

    global_attrs = dataset.attrs
    variable_attrs = dataset[variable].attrs

    # Assume that the raster matrix is always in lat, lon format

    #Look at the word lat, lon, latitude or longitude (case insensitive)
    if dims[-2].lower() in ['lat', 'latitude'] and dims[-1].lower() in ['lon', 'longitude']:
        return 'EPSG:4326'
    
    # check global and variable attributes for lat, lon hints
    lat_found = False
    lon_found = False

    for key, value in  global_attrs.items():
        
        if not isinstance(key, str) or not isinstance(value, str):
            continue 
        
        if key.lower().startswith('lat') or key.lower().startswith('latitude'):
            if value.lower().startswith(dims[-2].lower()):
                lat_found = True

        if value.lower().startswith('lat') or value.lower().startswith('latitude'):
            if key.lower().startswith(dims[-2].lower()):
                lat_found = True

        if key.lower().startswith('lon') or key.lower().startswith('longitude'):
            if value.lower().startswith(dims[-1].lower()):
                lon_found = True

        if value.lower().startswith('lon') or value.lower().startswith('longitude'):
            if key.lower().startswith(dims[-1].lower()):
                lon_found = True

    for key, value in  variable_attrs.items():

        if not isinstance(key, str) or not isinstance(value, str):
            continue 

        if key.lower().startswith('lat') or key.lower().startswith('latitude'):
            if value.lower().startswith(dims[-2].lower()):
                lat_found = True

        if value.lower().startswith('lat') or value.lower().startswith('latitude'):
            if key.lower().startswith(dims[-2].lower()):
                lat_found = True

        if key.lower().startswith('lon') or key.lower().startswith('longitude'):
            if value.lower().startswith(dims[-1].lower()):
                lon_found = True

        if value.lower().startswith('lon') or value.lower().startswith('longitude'):
            if key.lower().startswith(dims[-1].lower()):
                lon_found = True

    if lat_found and lon_found:
        return 'EPSG:4326'

    min_lat = math.ceil(dataset[dims[-2]].min().values.item())
    max_lat = math.ceil(dataset[dims[-2]].max().values.item())
    min_lon = math.ceil(dataset[dims[-1]].min().values.item())
    max_lon = math.ceil(dataset[dims[-1]].max().values.item())

    if -180 <= min_lon <= -175 and 175 <= max_lon <= 180 and -90 <= min_lat <= -85 and 85 <= max_lat <= 90:
        return 'EPSG:4326'

    return 'EPSG:4326' # Default

    

def get_no_data(dataset, variable):
    no_data_value = dataset[variable].rio.nodata

    if no_data_value != None:
        return no_data_value

    no_data_value = dataset[variable].attrs.get('_FillValue')
    if no_data_value != None:
        return no_data_value
    no_data_value = dataset[variable].attrs.get('missing_value')
    if no_data_value != None:
        return no_data_value

def get_units(dataset, variable):
    units = dataset[variable].attrs.get('units')

    if not units:
        units = dataset.attrs.get('units')
    
    if not units:
        raise ValueError("no unit definition found")

    if not isinstance(units, str):
        if isinstance(units, (tuple, list)) and len(units) > 0:
            units = units[0]
            if not isinstance(units, str):
                raise ValueError("Unable to determine unit")

    return units


def get_bounds(dataset, variable):

    dims = dataset[variable].dims
    
    min_lat = dataset[dims[-2]].min().values.item()
    max_lat = dataset[dims[-2]].max().values.item()
    min_lon = dataset[dims[-1]].min().values.item()
    max_lon = dataset[dims[-1]].max().values.item()

    bounds = (
        min_lon, 
        min_lat, 
        max_lon, 
        max_lat,
    )

    width = len(dataset[variable][dims[-1]])
    height = len(dataset[variable][dims[-2]])

    return {
        'bounds': bounds,
        'width': width,
        'height': height
    }


def calculate_band_number(indices, sizes):
    """
    Calculate the band number for a multi-dimensional array.
    
    Parameters:
        indices (list): A list of indices corresponding to the band dimensions.
        sizes (list): A list of sizes for each of the band dimensions.
    
    Returns:
        int: The calculated band number.
    """
    N_B = len(indices)  # Number of band dimensions
    band_number = 0

    for j in range(N_B):
        # Calculate the product of sizes for the remaining dimensions
        product = 1
        for k in range(j + 1, N_B):
            product *= sizes[k]
        
        band_number += indices[j] * product
    
    return band_number


with tempfile.NamedTemporaryFile(delete=False) as temp_file:
# if True:
    temp_file_path = f"{temp_file.name}.nc"
    # temp_file_path = 'FN.nc'

    print(temp_file_path)

    print("Downloading..")
    wget.download(file_url, temp_file_path)
    print("\nDownloaded")

    dataset = xr.open_dataset(
        temp_file_path, 
        engine="rasterio", 
        mask_and_scale=False
    )

    variables = dataset.data_vars

    for variable in variables:
        variable_dims = dataset.data_vars[variable].dims
        
        if len(variable_dims) < 3:
            continue


        band_index_dims_size = [ len(dataset[variable][dim]) for dim in variable_dims[0:-2]]

        
        def next_(iteratr):
            try:
                return next(iteratr)
            except StopIteration:
                return None

        band_dims_iterator = iter(dataset.data_vars[variable].dims[0:-2])

        lis = []

        def combine(itr, lis):

            for index, item in enumerate(itr):
                
                lis.append((item, index))
        
                next_band_dim = next_(band_dims_iterator)
                
                if next_band_dim:
                    combine(
                        iter(dataset.data_vars[variable][band_dims_iterator]),
                        lis
                    )
                else:
                    if len(lis) == len(
                        dataset.data_vars[variable].dims[0:-2]
                    ):
                        yield lis
                        lis = []
        
        band_dim = next_(band_dims_iterator)

        if band_dim:
            band_iterator = combine(
                iter(dataset.data_vars[variable][band_dim]),
                lis
            )
            

            for band_index_list in band_iterator:

                band = dataset.data_vars[variable]

                append_name = f"__{variable}"

                band_id_list = []
                band_indexes = []
                
                for band_name, band_index in band_index_list:

                    append_name += f"__{str(band_name.values)}"

                    band_id_list.append(str(band_name.values))
                    band_indexes.append(band_index)

                

                bidx = calculate_band_number(band_indexes, band_index_dims_size) + 1

                crs = get_crs(dataset, variable)
            
                if crs == None:
                    raise ValueError("CRS not found")

                no_data_value = get_no_data(dataset, variable)

                if no_data_value == None:
                    raise ValueError("Cannot detect nodata value")

                unit = get_units(dataset, variable)

                print(crs)
                print(no_data_value)
                print(unit)


                bounds = get_bounds(dataset, variable)
                
                dst_profile = cog_profiles.get("deflate")
                dst_profile.update(dict(BIGTIFF="IF_SAFER"))
        
                
                cog_translate(
                    temp_file_path,                                    # Source file (in-memory GeoTIFF)
                    f"my-output-cog{append_name}.tif",      # Output file path
                    dst_profile,
                    indexes=bidx,                            # COG profile settings
                    config={
                        "GDAL_NUM_THREADS": "ALL_CPUS",
                        "GDAL_TIFF_INTERNAL_MASK": True,
                        "GDAL_TIFF_OVR_BLOCKSIZE": "128",
                    },
                    in_memory=False,                         # Keep in memory to avoid disk I/O
                    quiet=False,                             # Suppress logs
                    nodata=no_data_value,
                    additional_cog_metadata=dict(
                        variable=variable,
                        unit=unit
                    )
                )




        


    
    

    
