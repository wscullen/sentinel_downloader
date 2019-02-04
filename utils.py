def get_utm_tile(product_name):
    """Get the UTM zone and number from the product name.

    Utility func to extract the MGRS tile id values from a product name in
    the form:
    ``S2A_MSIL1C_20170404T183821_N0204_R027_T12UVF_20170404T183816``
    where ``T12UVF`` is the relevant part of the string we are looking for.

    If the product name is in the form:
    ``S2A_OPER_PRD_MSIL1C_PDMC_20160101T001237_\
    R027_V20151231T184606_20151231T184606``
    This means that the product is in an older format with multiple GRANULES
    per product, which means that the individual granule names would contain
    the MGRS information, not the product name. Returns a ``namedtuple`` with
    the tile information, accessible with dot notation.

    Args:
        product_name (str): Name of the product to extract MGRS info from.

    Returns:
        (namedtuple): A namedtuple of type MGRSTile, with the properties
        zone (UTM zone number), band (MGRS latitude band number), squareID
        (MGRS 100km 2-letter square ID), and string (str repr of tile info).

    """

    tile_id = re.search(r'T\d{2}[A-Z]{3}', product_name)

    if not tile_id:
        # No match found, older data format
        return None

    # Using a namedtuple from collections to do dot notation

    utm_zone_num = tile_id.group(0)[1:3]
    utm_latitude_band = tile_id.group(0)[3:4]
    utm_100km_zone_id = tile_id.group(0)[4:6]

    tile = {}

    tile['zone'] = utm_zone_num
    tile['band'] = utm_latitude_band
    tile['square'] = utm_100km_zone_id
    tile['full'] = "{}{}_{}".format(utm_zone_num,
                                     utm_latitude_band,
                                     utm_100km_zone_id)

    return tile

def convert_and_move(filename, date, atmos_cor):
    """ Use GDAL to convert to .tif, simplify name of result.

    This function uses a helper function that utilizes the GDAL library
    to convert the downloaded and \
    corrected .jp2 to .tif, and rename the result using a simpler format \
    and folder structure compared to the original .SAFE structure::

        S2A_MSIL2A_20170517T152631_N0205_R068_T20TMS_20170517T152629.SAFE
        -->
        GRANULE
        -->
        L2A_T20TMS_A009932_20170517T152629
        -->
        IMG_DATA
        -->
        R20m
        -->
        L2A_T20TMS_20170517T152631_TCI_20m.jp2

    Becomes::

        S2_20_T_MS_20170517
        -->
        S2_20_T_MS_20170517_TCI_20m.tif

    Once the image files are converted and moved, the metadata file from the \
    granule folder is copied and renamed, and finally the entire folder is \
    compressed using the ``zipfile`` module. Before beginning conversion, the
    characters prior to the suffix on the first .tif encountered are looked at,
    if the same atmospheric correction value exists, conversion process is
    aborted. Otherwise the original zip file is deleted and the process continues
    allowing for the newly converted resolutions to be added to the zip archive.

    Args:
        filename (str): Name of the folder containing the product, in the form \
            ``S2A_MSIL2A_20170517T152631_N0205_R068_T20TMS_20170517T152629.SAFE``
        date (Date): Date object representing the 'beginposition' date of the \
            acquisition of imagery.

    Returns:
        str: "Already converted" if the target directory already exists for
        the renamed and converted files, or "Finished" messages if successful.

    """
    # TODO: Something is wrong with this function, getting false positives
    # for files already existing, so the conversion process is aborted
    # when it shouldn't be

    logger.debug('Starting conversion process for {}'.format(filename))
    tqdm.tqdm.write('Starting conversion process for {}'.format(filename))

    # Get tile id using re, break it out into zone num, tile ids, MGRS format
    tile = get_utm_tile(filename)

    if not tile:
        tile_zone = 'none'
        tile_band = 'none'
        tile_squareID = 'none'
    else:
        tile_zone = tile['zone']
        tile_band = tile['band']
        tile_squareID = tile['square']
    # check if converted directory already exists
    destination_dir = "S2_{}_{}_{}_{}".format(tile_zone,
                                                  tile_band,
                                                  tile_squareID,
                                                  date.strftime('%Y%m%d_%H%M'))

    # Check if the simplified directory already exists
    if Path.exists(Path(FINAL_DIR, destination_dir)):
        logger.debug('Final result directory already exists, checking file'
                    'extension of first file to see if it matches the current'
                    'atmospheric correction value')

        file_list_iter = os.scandir(Path(FINAL_DIR, destination_dir))


        for file_name in file_list_iter:
            if file_name.name[-7:-5] == str(atmos_cor):
                logger.debug('That resolution of atmos correction already'
                             'exists..., aborting the conversion...')
                logger.debug('Checking if the .zip still exists...')

                if not (Path(FINAL_DIR, destination_dir + '.zip').exists()):
                    zip_directory(destination_dir)

                return "already exists"

            if not file_name.name[-7:-5] in ['10','20','60'] and atmos_cor == 0:
                logger.debug('Uncorrected files already exist, aborting...')

                logger.debug('Checking if the .zip still exists...')

                if not (Path(FINAL_DIR, destination_dir + '.zip').exists()):
                    zip_directory(destination_dir)

                return "already exists"

    logger.info('Final result directory does not exist. Beginning conversion')
    logger.debug('atmos_cor: {}'.format(atmos_cor))

    if atmos_cor in [10, 20, 60]:
        # Convert filename to L2A with .SAFE suffix
        file_string = filename.replace('L1C', 'L2A')
    else:
        file_string = filename

    file_string += '.SAFE'

    # Make sure the temporary data folder we are converting exists
    if not os.path.isdir(os.path.join(BUNDLE_DIR, TEMP_DIR, file_string)):
        logger.warning('Expected data directory to be converted does not exist'
                       ' exiting...')
        tqdm.tqdm.write('Expected data directory to convert does not exist, stopping...')

        return 'failed'

    granule_dir = Path('temp', file_string, 'GRANULE')

    # Iterate over each granule, if there are multiple
    for dir in granule_dir.iterdir():

        logger.debug(str(dir))
        if atmos_cor in [10, 20, 60]:
            convert_dir = Path(dir, 'IMG_DATA', "R{}m".format(atmos_cor))
        else:
            convert_dir = Path(dir, 'IMG_DATA',)

        logger.debug(convert_dir)
        file_list = convert_dir.glob('*.jp2')

        # For the actual img data, use the convert_jp2 function
        for file in file_list:
            logger.debug(file)
            dest_path = Path(FINAL_DIR, destination_dir)
            try:
                conv.convert_jp2_to_tif(file,
                                    destination_dir,
                                    atmos_cor,
                                    dest_path)
            except Exception as e:
                logger.error('Something went wrong with the conversion')
                logger.error(str(e))
                return 'failure'

        # Copy and rename the metadata file in the GRANULE
        shutil.copy2(str(Path(dir, 'MTD_TL.xml')),
                  str(Path(FINAL_DIR,
                           destination_dir,
                           destination_dir + "_metadata.xml")))

        logger.info('converted jp2 to tif successfully, zipping result')
        zip_directory(destination_dir)

    return 'success'


def zip_directory(dirname):
    """ Small wrapper function that uses the ZipFile module.

    Args:
        dirname (str): The name of the new folder inside FINAL_DIR that has \
            all the renamed and converted data.

    """

    if (Path(FINAL_DIR, dirname + '.zip').exists()):
        logger.debug('Zip archive already exists. Deleting existing archive')
        os.remove(Path(FINAL_DIR, dirname + '.zip'))

    logger.debug('Starting zip process. Zipping...')
    zipf = zipfile.ZipFile(str(Path(FINAL_DIR, dirname + '.zip')), 'w', zipfile.ZIP_DEFLATED)

    for root, dirs, files in os.walk(str(Path(FINAL_DIR, dirname))):
        for file in files:
            zipf.write(os.path.join(root, file), file)

    zipf.close()
