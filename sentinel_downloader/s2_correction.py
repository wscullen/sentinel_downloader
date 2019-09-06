import os
import subprocess
import logging
import platform
from osgeo import gdal

gdal.UseExceptions()

from s2d2 import utils

logger = logging.getLogger(__file__)

if platform.system() == 'Windows':
    PLATFORM = 'windows'
elif platform.system() == 'Linux':
    PLATFORM = 'linux'


def validate_correction(product_path, ac_resolution):
    """ Validate the correction by sen2cor.

    Sometimes the correction will return successfully but not have completed
    properly. This should be marked as a failure with an exception or return code
    other than 0.

    At 10m, there should be 35 .tifs in the IMG_DATA dir.
    At 20m and 60m, there should be 28.

    Therefore if we walk the GRANULE\L2A_T18TVQ_A006415_20180529T160302\IMG_DATA
    dir, we can count the total tifs, in addition to trying to open each one with GDAL.
    If the count is off, or GDAL count open any of the tif, we mark the correction as failed.

    Note: If the correction fails three times, we can assume there is a problem
    with the raw download. That should be deleted and the entire process should be started again.

    """
    # Get the path to the IMG_DATA folder
    img_data_parent = os.listdir(os.path.join(product_path, 'GRANULE'))[0]
    img_data_full_path = os.path.join(
        product_path, 'GRANULE', img_data_parent, 'IMG_DATA')

    # Walk the IMG_DATA folder and collect a list of all .tif files in it
    # Check if each of the 10, 20, 60 resolution folders exist, if they do, count them

    tif_list_10m = []
    tif_list_20m = []
    tif_list_60m = []

    # 10m
    img_data_10m = os.path.join(img_data_full_path, 'R10m')
    if os.path.isdir(img_data_10m):
        for path, subdirs, files in os.walk(img_data_10m):
            for f in files:
                print(path, subdirs, f)
                file_name, file_extension = os.path.splitext(f)
                if file_extension == '.jp2':
                    tif_list_10m.append(os.path.join(path, f))
    
    # 20m
    img_data_20m = os.path.join(img_data_full_path, 'R20m')
    if os.path.isdir(img_data_20m):
        for path, subdirs, files in os.walk(img_data_20m):
            for f in files:
                print(path, subdirs, f)
                file_name, file_extension = os.path.splitext(f)
                if file_extension == '.jp2':
                    tif_list_20m.append(os.path.join(path, f))

    # 60m
    img_data_60m = os.path.join(img_data_full_path, 'R60m')
    if os.path.isdir(img_data_60m):
        for path, subdirs, files in os.walk(img_data_60m):
            for f in files:
                print(path, subdirs, f)

                file_name, file_extension = os.path.splitext(f)
                if file_extension == '.jp2':
                    tif_list_60m.append(os.path.join(path, f))

    # Count the files, verify they match
    if tif_list_10m:
        if len(tif_list_10m) != 7:
            return (False, f'Missing 10m bands ({len(tif_list_10m)}/7)')
    elif ac_resolution == 10:
        return (False, f'Missing 10m image folder in granule (required for AC resolution of {ac_resolution}')

    if tif_list_60m:
        if len(tif_list_60m) != 15:
            return (False, f'Missing 60m bands ({len(tif_list_60m)}/15)')
    elif ac_resolution == 60:
        return (False, f'Missing 60m image folder in granule (required for AC resolution of {ac_resolution}')

    if tif_list_20m:
        if len(tif_list_20m) != 13:
            return (False, f'Missing 20m bands ({len(tif_list_20m)}/13)')
    else:
        return (False, f'Missing 20m image dir in granule, it must always be present regardless of AC resolution specified.')
    
    # Create a single list of all image files, try to open each one with GDAL
    all_img_files = tif_list_10m + tif_list_20m + tif_list_60m

    for img in all_img_files:
        print(img)
        try:
            ds = gdal.Open(img)
        except BaseException as e:
            print('GDAL could not open file, failed integrity check')
            print(e)
            print(f'failed on {f}')
            return (False, f'corrupt or invalid .jp2, or wrong path for .jp2')
        else:
            ds = None

    # All .jp2 were opened succesfully, the product is valid
    return (True, f'Valid product')


def correct_product(product_id, folder, ac_resolution, use_dem):
    """ Runs the sen2cor correction process using subprocess

    This is a simple wrapper for calling the command line tool sen2cor.
    This function uses ``subprocess`` module to call a local version of
    sen2cor in the "sen2cor240" folder. It uses the function args to
    specify the folder to save the atmospherically corrected product
    (should be TEMP_DIR), and to set the resolution of the correction (20m
    is a good default).

    .. note:: Here would be where a user supplied L2A_GIPP.xml would be \
    specified, which would allow the DEM correction to be performed.

    Args:
        product_id (str): Title of the product, in the form: \
        ``S2A_MSIL1C_20170404T183821_N0204_R027_T12UVF_20170404T183816``

        folder (str): Location to save the corrected .SAFE folder, should be \
        ``TEMP_DIR``

        ac_resolution (int): Resolution to perform the atmospheric correction \
        at. 20 is usually a good compromise between time and accuracy.

    Returns:
        str: 'success' if everything went well, or 'failed (Exception)' if \
        something went wrong.
    """

    result = None
    print(product_id)
    print('inside correct_product')
    print(os.path.join(folder, product_id))

    source_dir = os.path.join(folder, product_id)
    print(source_dir)

    if PLATFORM == 'windows':
        sen2cor_command = 'L2A_Process.bat'
    elif PLATFORM == 'linux':
        sen2cor_command = 'L2A_Process'
    else:
        print('UNSUPPORTED PLATFORM')
        return utils.TaskStatus('failed', 'Unsupported platform for sen2cor', None)

    if os.path.isdir(os.path.join(folder, product_id)):

        try:
            # TODO: Implement retry incase of failures

            # since the exit code never changes from 0, we can listen
            # to the std error and report a failure if the output is in
            # stderr instead of stdout
            argument_list = None
            if ac_resolution == 10:
                argument_list = [sen2cor_command,
                                 source_dir]
            else:
                argument_list = [sen2cor_command,
                                 source_dir,
                                 '--resolution', str(ac_resolution)]

            subprocess_result = subprocess.run(argument_list)

        except Exception as e:
            logger.debug('Something went wrong with correction: {}'.format(e))
            print(f'Something went wrong during correction. {e}')
            return utils.TaskStatus(
                False, 'Something went wrong during correction', str(e))
        else:
            if subprocess_result.returncode != 0:
                return utils.TaskStatus(False, 'Error occured within sen2cor', None)
            else:
                valid_correction = validate_correction(
                    source_dir.replace('L1C', 'L2A'), ac_resolution)
                
                if valid_correction[0]:
                    print('product corrected and validated.')
                    return utils.TaskStatus(True, 'Product corrected and validated', None)
                else:
                    print('product corrected and validated.')
                    return utils.TaskStatus(False, valid_correction[1], None)

    else:
        return utils.TaskStatus(False, 'Data folder does not exist', None)