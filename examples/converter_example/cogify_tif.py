import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# Input and output file paths
input_tif = "FN.tif"  # Input GeoTIFF file
output_directory = "output"  # Directory where output COGs will be saved

# Open the source file to inspect both global and band-level metadata
with rasterio.open(input_tif) as src:
    # Get global metadata (for the entire file)
    global_metadata = src.tags()  # Global metadata for the whole file
    
    # Check for 'variable' and 'unit' in the global metadata
    global_variable = global_metadata.get('variable', 'Not available')
    global_unit = global_metadata.get('unit', 'Not available')
    
    # Print global metadata
    print("Global Metadata:")
    print(f"  Variable: {global_variable}")
    print(f"  Unit: {global_unit}")
    print(f"Nodata: {src.nodata}")
    
    # Read the number of bands in the file
    num_bands = src.count
    print(f"\nNumber of bands in the input file: {num_bands}")
    
    # Create COG profile (you can adjust compression and blocksize as needed)
    dst_profile = cog_profiles.get("deflate")  # Use "deflate" compression for example

    dst_profile.update(dict(BIGTIFF="IF_SAFER"))
    
    # Process each band
   
    for band_index in range(1, num_bands + 1):
        

        # Get metadata (tags) for the current band
        band_metadata = src.tags(band_index)
        
        # Check for 'variable' and 'unit' in the band metadata
        band_variable = band_metadata.get('variable', 'Not available')
        band_unit = band_metadata.get('unit', 'Not available')
        
        # Print band-specific metadata
        print(f"\nBand {band_index} Metadata:")
        print(f"  Variable: {band_variable}")
        print(f"  Unit: {band_unit}")
        
        # Prepare metadata for the band (can include global metadata if needed)
        # Here we use both global and band-specific metadata
        
        if num_bands == 1:
            band_variable = band_variable if band_variable else global_variable
            band_unit = band_unit if band_unit else global_unit
        
        additional_cog_metadata = {
            # "variable": band_variable,
            # "unit": band_unit,
            "variable": 'forest_cover',
            "unit": "adimensional"
        }
        
        # Define output file path for the individual band COG
        output_band_path = f"band_{band_index}_output_cog.tif"

        dst_profile.update({
            "dtype": "float32",  # Use the correct data type
            "nodata": 0,  # Ensure nodata is preserved
            "blockxsize": 128,
            "blockysize": 128,
        })
        
        # Apply cog_translate to convert each band to COG
        cog_translate(
            input_tif,  # Source file
            output_band_path,  # Output file path for this band
            dst_profile,
            indexes=[band_index],  # Process only the current band
            nodata=0,  # Set the nodata value for this band
            config={
                "GDAL_NUM_THREADS": "ALL_CPUS",  # Use all CPU cores for processing
                "GDAL_TIFF_INTERNAL_MASK": True,  # Enable internal masks for transparency
                "GDAL_TIFF_OVR_BLOCKSIZE": "128",  # Block size for overviews
            },
            in_memory=False,  # Keep file processing on disk
            quiet=False,  # Verbose output for debugging
            additional_cog_metadata={**additional_cog_metadata}  # Merge global and band metadata
        )
        
        print(f"COG for band {band_index} saved as {output_band_path}")
