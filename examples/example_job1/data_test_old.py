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

# import rpy2.robjects as robjects
# from rpy2.robjects.packages import importr

# Load the terra package
# terra = importr('terra')

ssl._create_default_https_context = ssl._create_unverified_context

file_url = 'https://localip:9000/accelerator/brightspace/SpatialX/FN.nc'

#Look at the word lat, lon, latitude or longitude (case insensitive)

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


# with tempfile.NamedTemporaryFile(delete=False) as temp_file:
if True:
    # temp_file_path = f"{temp_file.name}.nc"
    temp_file_path = 'FN.nc'

    print(temp_file_path)

    print("Downloading..")
    # wget.download(file_url, temp_file_path)
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
                
                for band_name, band_index in band_index_list:

                    append_name += f"__{str(band_name.values)}"

                    band_id_list.append(str(band_name.values))
                    
                    band = band[band_index]
                
                # print(band)

                crs = get_crs(dataset, variable)
            
                if crs == None:
                    raise ValueError("CRS not found")

                no_data_value = get_no_data(dataset, variable)

                if no_data_value == None:
                    raise ValueError("Cannot detect nodata value")

                units = get_units(dataset, variable)

                print(crs)
                print(no_data_value)
                print(units)


                bounds = get_bounds(dataset, variable)




                src_transform = from_bounds(
                    *bounds.get('bounds'), 
                    width=bounds.get('width'), 
                    height=bounds.get('height')
                )

                src_profile = dict(
                    driver="GTiff",
                    dtype="float32",       # Make sure dtype matches your data (e.g., float32)
                    count=1,               # Number of bands (1 in this case)
                    height=bounds.get('height'),
                    width=bounds.get('width'),
                    crs=crs,               # Coordinate Reference System
                    transform=src_transform, # Transformation (from bounds or affine)
                    nodata=no_data_value,    # NoData value for the dataset
                )

                import pdb
                pdb.set_trace()

                with MemoryFile() as memfile:
                    with memfile.open(**src_profile) as mem:
                        # Populate the input file with numpy array

                        
                        
                        band_data = band.values
                        
                        mem.write(band_data, 1)

                        valid_data = np.ma.masked_equal(band_data, no_data_value)

                        min_val = np.nanmin(valid_data)  # Minimum value ignoring NaNs
                        max_val = np.nanmax(valid_data)  # Maximum value ignoring NaNs
                        mean_val = np.nanmean(valid_data)  # Mean value ignoring NaNs
                        std_val = np.nanstd(valid_data)

                        mem.update_tags(
                            variable=variable,
                            units=units,
                            band_info='-'.join(band_id_list)
                        )

                        mem.update_tags(
                            STATISTICS_MINIMUM=min_val,
                            STATISTICS_MAXIMUM=max_val,
                            STATISTICS_MEAN=mean_val,
                            STATISTICS_STDDEV=std_val,
                            NODATA=no_data_value  # Add nodata information
                        )

                        # overviews = [2, 4, 8, 16, 32]  # Example overview levels

                        # # Build overviews for the in-memory file
                        # mem.build_overviews(overviews, resampling=rasterio.enums.Resampling.bilinear)

                        # # Update the metadata to include the overview information
                        # mem.update_tags(OVERVIEWS=' '.join(map(str, overviews)))

                        dst_profile = cog_profiles.get("deflate")
                        dst_profile.update(dict(BIGTIFF="IF_SAFER"))
                        
                        cog_translate(
                            mem,                                    # Source file (in-memory GeoTIFF)
                            f"my-output-cog{append_name}.tif",      # Output file path
                            dst_profile,                            # COG profile settings
                            config={
                                "GDAL_NUM_THREADS": "ALL_CPUS",
                                "GDAL_TIFF_INTERNAL_MASK": True,
                                "GDAL_TIFF_OVR_BLOCKSIZE": "128",
                            },
                            in_memory=True,                         # Keep in memory to avoid disk I/O
                            quiet=False,                             # Suppress logs
                            nodata=no_data_value  
                        )




        


    
    

    
