## Overview

This is a helper module that interacts with the ESA Scihub website for downloading S2 satellite imagery.

## Extra Utilities

Also included are some extra utility scripts used for converting between WRS and MGRS tile systems. Use the `shapefile_to_wrs.py` script to create a shapefile with all the WRS tiles that overlap with a given shapefile. You can use this WRS shapefile to generate a .csv lookup table going from WRS pathrow to a list of MGRS tiles.

Generate a WRS intersection with arbirtary shapefile:
`python shapefile_to_wrs.py -shapefile ./data/canada_extent.shp -wrs_intersects`

Generate a csv lookup from WRS to MGRS:
`python shapefile_to_wrs.py -shapefile ./data/intersecting_wrstiles.shp -wrs_to_mgrs`

## Install GDAL

Make sure GDAL 2.X is installed on the system, and then while in your pipenv environment, run:

```
pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`
```

## Install Other Requirements

You can use the included Pipfile to install the rest of the requirements after GDAL, run it with `pipenv install`

## Required Data Files

Shapefiles for the WRS and MGRS grids are required to lookup and convert between the two systems. Download the files from here:

[landsat_downloader_data.zip](https://drive.google.com/file/d/14lqY25kH1sU2kVYO6yR6ASPrDWW3fQ3J/view?usp=sharing)

`grid_files` and `data` directories goes under the main project directory, `test_data` directory goes under the `test` directory in the main project directory.

## Env Vars for SCIHUB AUTH

Make sure to set SCIHUB_USERNAME and SCIHUB_PASSWORD to the usernamd and password that you use to access Scihub.

## Install Directly From Github

`python3.8 -m pip install git+https://sscullen:$GITHUB_PAT@github.com/wscullen/sentinel_downloader.git#egg=sentinel_downloader`

or to upgrade:

`python3.8 -m pip install --upgrade git+https://sscullen:$GITHUB_PAT@github.com/wscullen/sentinel_downloader.git#egg=sentinel_downloader`

# testing tagging now with manual 1.0.0 version added
