
import argparse
import json
import logging


def parse_cli_args():
    """ Get the cmd line arguments using "argparse" library.

    module docstring for all the command line options available.

    """
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
                description='Download Sentinel2 data for region specified \
                             by a polygon. Specify by cloud %, date range,\
                             and polygonal extent.')

    parser.add_argument('-p',
                        dest="json_product", action='store',
                        type=str,
                        help='json string repr product to preprocess')

    parser.add_argument('-util_path',
                        dest="util_path", action='store',
                        type=str,
                        help='path to the sentinel_downloader module')

    parser.add_argument('-td', dest='temp_data_dir',
                        action='store',
                        help='Date range to retrieve imagery. \
                              Specify start and end dates (YYYYMMDD) \
                              separated by space.')

    parser.add_argument('-demd', dest='dem_data_dir',
                        action='store',
                        help='Date range to retrieve imagery. \
                              Specify start and end dates (YYYYMMDD) \
                              separated by space.')


    arg_object = parser.parse_args()

    return arg_object

if __name__ == "__main__":

    args_obj = parse_cli_args()

    product = json.loads(args_obj.json_product)

    footprint = product['footprint']

    temp_data_dir = args_obj.temp_data_dir

    external_dem_dir = args_obj.dem_data_dir

    path_to_s2_module = args_obj.util_path

    import sys
    sys.path.insert(0, path_to_s2_module)

    from sentinel_downloader import s1_utils


    s1_preprocessor = s1_utils.S1_Preprocessor(product,
                                                footprint,
                                                temp_data_dir,
                                                external_dem_dir)


    s1_preprocessor.apply_orbit_file(write_intermediate=False)

    # TODO needs to be finished
    # TODO need to add sci hub fallback for downloads for both L8 and S1
    s1_preprocessor.speckle_filter(write_intermediate=False)

    # s1_preprocessor.range_doppler_to_sigma0(write_intermediate=False)

    s1_preprocessor.write_out_result(format='BEAM-DIMAP')