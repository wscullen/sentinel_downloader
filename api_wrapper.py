from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
from sentinel_downloader import s2_downloader

from osgeo import ogr

import tqdm
import logging

from datetime import datetime
from datetime import timedelta

logger = logging.getLogger(__name__)

# TODO: Not functional as is. Needs work.


def query_by_polygon(platform_name, polygon_list, arg_list, date_string, config_path=None):
    """ platform_name can be ['Sentinel-1', 'Sentinel-2', 'Landsat-8']
        polygon_list is a list of wkt polygons
        arg_list has
            all
            ['date_start', 'date_end', 'raw_coverage']
            Sentinel-1
            ['product_type', 'sensor_mode', 'resolution']
            Sentinel-2
            ['cloud_percent', 'coverage_minus_cloud']
            Landsat-8
            ['cloud_percent', 'coverage_minus_cloud']
    """

    products_dict = {}

    # args that apply to all products
    arg_dict = {
        'date': (arg_list['date_start'], arg_list['date_end'] + timedelta(days=1)),
        'platformname': platform_name,
    }

    if platform_name == 'Sentinel-1':

        if 'product_type' in arg_list:
            arg_dict['producttype'] = arg_list['product_type']

        if 'sensor_mode' in arg_list:
            arg_dict['sensoroperationalmode'] = arg_list['sensor_mode']

        if 'resolution' in arg_list:
            arg_dict['filename'] = 'S1?_??_???{}_*'.format(
                arg_list['resolution'])

        logger.info('Querying Copernicus API for S1 with args: %s' % arg_dict)
        print(arg_dict)
        for index, fp in enumerate(polygon_list):
            products = None

            try:
                s2_dl = s2_downloader.S2Downloader(config_path)

                products = s2_dl.search_for_products(
                    platform_name, fp, arg_dict)

            except Exception as e:
                logger.debug(
                    'Error occured while trying to query API: {}'.format(e))
                print('Sorry something went wrong while trying to query API')
                raise
            else:
                if products:
                    for key, value in products.items():

                        product_dict = {}
                        product_dict['entity_id'] = key

                        # S1 specific metadata
                        product_dict['sensor_mode'] = value['sensoroperationalmode']
                        product_dict['polarization_mode'] = value['polarisationmode']
                        product_dict['product_type'] = value['producttype']

                        product_dict['detailed_metadata'] = value
                        product_dict['api_source'] = 'esa_copernicus'
                        product_dict['download_source'] = None
                        product_dict['footprint'] = value['footprint']

                        product_dict['acquisition_start'] = value['beginposition']

                        product_dict['acquisition_end'] = value['endposition']

                        geom = ogr.CreateGeometryFromWkt(
                            product_dict['footprint'])
                        # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                        env = geom.GetEnvelope()

                        def envelope_to_wkt(env_tuple):
                            coord1 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[3])
                            coord2 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[3])
                            coord3 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[2])
                            coord4 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[2])

                            wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(
                                coord1, coord2, coord3, coord4, coord1)
                            return wkt_string

                        product_dict['mbr'] = envelope_to_wkt(env)

                        product_dict['dataset_name'] = 'S2MSI1C'
                        product_dict['name'] = value['title']
                        product_dict['sat_name'] = 'Sentinel-1A' if product_dict['name'][2] == 'A' else 'Sentinel-1B'
                        product_dict['vendor_name'] = value['identifier']
                        product_dict['uuid'] = key

                        product_dict['preview_url'] = value['link_icon']
                        product_dict['manual_product_url'] = value['link']
                        product_dict['manual_download_url'] = value['link_alternative']
                        product_dict['manual_bulkorder_url'] = None
                        # TODO: create a link to teh metaddata files using http get request
                        product_dict['metadata_url'] = None
                        product_dict['last_modified'] = value['ingestiondate']
                        product_dict['bulk_inprogress'] = None
                        product_dict['summary'] = value['summary']

                        # TODO: write a conversion module for converting between pathrow and MGRS centroids (nearest neighbor or most coverage)
                        product_dict['pathrow'] = None

                        # TODO: calculate this value once the atmos and scene classes are done
                        product_dict['land_cloud_percent'] = None

                        product_dict['cloud_percent'] = None

                        product_dict['platform_name'] = value['platformname']
                        product_dict['instrument'] = value['instrumentshortname']

                        # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                        # TODO: S1 does not come with a tile id, look up through shapefiles
                        product_dict['mgrs'] = None
                        product_dict['orbit'] = value['relativeorbitnumber']
                        product_dict['abs_orbit'] = value['orbitnumber']

                        products_dict[key] = product_dict

    elif platform_name == 'Sentinel-2':

        if 'cloud_percent' in arg_list:
            arg_dict['cloudcoverpercentage'] = (0, arg_list['cloud_percent'])

        print('Querying Copernicus API for S2 with args: %s' % arg_dict)

        for index, fp in enumerate(polygon_list):
            products = None
            print(fp)
            try:
                s2_dl = s2_downloader.S2Downloader(config_path)

                products = s2_dl.search_for_products(
                    platform_name, fp, arg_dict)
            except Exception as e:
                logger.debug(
                    'Error occured while trying to query API: {}'.format(e))
                print('Sorry something went wrong while trying to query API')
                raise
            else:
                print('inside api wrapper')
                print(products)
                for p in products.items():
                    print(p)
                if products:
                    for key, value in products.items():
                        product_dict = {}
                        product_dict['entity_id'] = key

                        product_dict['detailed_metadata'] = value
                        product_dict['api_source'] = 'esa_copernicus'
                        product_dict['download_source'] = None
                        product_dict['footprint'] = value['footprint']

                        product_dict['acquisition_start'] = value['beginposition']

                        product_dict['acquisition_end'] = value['endposition']

                        geom = ogr.CreateGeometryFromWkt(
                            product_dict['footprint'])
                        # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                        env = geom.GetEnvelope()

                        def envelope_to_wkt(env_tuple):
                            coord1 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[3])
                            coord2 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[3])
                            coord3 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[2])
                            coord4 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[2])

                            wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(
                                coord1, coord2, coord3, coord4, coord1)
                            return wkt_string

                        product_dict['mbr'] = envelope_to_wkt(env)

                        product_dict['dataset_name'] = 'S2MSI1C'
                        product_dict['name'] = value['title']
                        product_dict['uuid'] = key

                        product_dict['size'] = value['size'][0:-3]

                        product_dict['preview_url'] = value['link_icon']
                        product_dict['manual_product_url'] = value['link']
                        product_dict['manual_download_url'] = value['link_alternative']
                        product_dict['manual_bulkorder_url'] = None
                        # TODO: create a link to teh metaddata files using http get request
                        product_dict['metadata_url'] = None
                        product_dict['last_modified'] = value['ingestiondate']
                        product_dict['bulk_inprogress'] = None
                        product_dict['summary'] = value['summary']
                        product_dict['sat_name'] = value['platformserialidentifier']
                        product_dict['vendor_name'] = value['identifier']

                        # TODO: write a conversion module for converting between pathrow and MGRS centroids (nearest neighbor or most coverage)
                        product_dict['pathrow'] = None

                        # TODO: calculate this value once the atmos and scene classes are done
                        product_dict['land_cloud_percent'] = None

                        product_dict['cloud_percent'] = value['cloudcoverpercentage']

                        product_dict['platform_name'] = value['platformname']
                        product_dict['instrument'] = value['instrumentshortname']

                        # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                        if 'tileid' in value.keys():
                            product_dict['mgrs'] = value['tileid']
                        else:
                            product_dict['mgrs'] = 'n/a'
                        product_dict['orbit'] = value['relativeorbitnumber']
                        product_dict['abs_orbit'] = value['orbitnumber']

                        products_dict[key] = product_dict

    else:
        logger.error('Invalid platform name!!!')

    return products_dict


def query_by_name(platform_name, name_list, arg_list, date_string, config_path=None):

    try:
        s2_dl = s2_downloader.S2Downloader(config_path)

        products = s2_dl.search_for_products_by_name(
            platform_name, name_list, arg_list)

    except Exception as e:
        logger.debug(
            'Error occured while trying to query API: {}'.format(e))
        print('Sorry something went wrong while trying to query API')
        raise
    else:
        products_dict = {}
        if products:

            print(products)
            if platform_name == 'Sentinel-1':
                    for key, value in products.items():
                        print(key)
                        print(value)
                        product_dict = {}
                        product_dict['entity_id'] = key

                        # S1 specific metadata
                        product_dict['sensor_mode'] = value['sensoroperationalmode']
                        product_dict['polarization_mode'] = value['polarisationmode']
                        product_dict['product_type'] = value['producttype']

                        product_dict['detailed_metadata'] = value
                        product_dict['api_source'] = 'esa_copernicus'
                        product_dict['download_source'] = None
                        product_dict['footprint'] = value['footprint']

                        product_dict['acquisition_start'] = value['beginposition']

                        product_dict['acquisition_end'] = value['endposition']

                        geom = ogr.CreateGeometryFromWkt(
                            product_dict['footprint'])
                        # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                        env = geom.GetEnvelope()

                        def envelope_to_wkt(env_tuple):
                            coord1 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[3])
                            coord2 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[3])
                            coord3 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[2])
                            coord4 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[2])

                            wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(
                                coord1, coord2, coord3, coord4, coord1)
                            return wkt_string

                        product_dict['mbr'] = envelope_to_wkt(env)

                        product_dict['dataset_name'] = 'S2MSI1C'
                        product_dict['name'] = value['title']
                        product_dict['sat_name'] = 'Sentinel-1A' if product_dict['name'][2] == 'A' else 'Sentinel-1B'
                        product_dict['vendor_name'] = value['identifier']
                        product_dict['uuid'] = key

                        product_dict['preview_url'] = value['link_icon']
                        product_dict['manual_product_url'] = value['link']
                        product_dict['manual_download_url'] = value['link_alternative']
                        product_dict['manual_bulkorder_url'] = None
                        # TODO: create a link to teh metaddata files using http get request
                        product_dict['metadata_url'] = None
                        product_dict['last_modified'] = value['ingestiondate']
                        product_dict['bulk_inprogress'] = None
                        product_dict['summary'] = value['summary']

                        # TODO: write a conversion module for converting between pathrow and MGRS centroids (nearest neighbor or most coverage)
                        product_dict['pathrow'] = None

                        # TODO: calculate this value once the atmos and scene classes are done
                        product_dict['land_cloud_percent'] = None

                        product_dict['cloud_percent'] = None

                        product_dict['platform_name'] = value['platformname']
                        product_dict['instrument'] = value['instrumentshortname']

                        # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                        # TODO: S1 does not come with a tile id, look up through shapefiles
                        product_dict['mgrs'] = None
                        product_dict['orbit'] = value['relativeorbitnumber']
                        product_dict['abs_orbit'] = value['orbitnumber']

                        products_dict[key] = product_dict

            elif platform_name == 'Sentinel-2':
                    for key, value in products.items():
                        product_dict = {}
                        product_dict['entity_id'] = key

                        product_dict['detailed_metadata'] = value
                        product_dict['api_source'] = 'esa_copernicus'
                        product_dict['download_source'] = None
                        product_dict['footprint'] = value['footprint']

                        product_dict['acquisition_start'] = value['beginposition']

                        product_dict['acquisition_end'] = value['endposition']

                        geom = ogr.CreateGeometryFromWkt(
                            product_dict['footprint'])
                        # Get Envelope returns a tuple (minX, maxX, minY, maxY)
                        env = geom.GetEnvelope()

                        def envelope_to_wkt(env_tuple):
                            coord1 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[3])
                            coord2 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[3])
                            coord3 = str(env_tuple[1]) + \
                                ' ' + str(env_tuple[2])
                            coord4 = str(env_tuple[0]) + \
                                ' ' + str(env_tuple[2])

                            wkt_string = "POLYGON(({}, {}, {}, {}, {}))".format(
                                coord1, coord2, coord3, coord4, coord1)
                            return wkt_string

                        product_dict['mbr'] = envelope_to_wkt(env)

                        product_dict['dataset_name'] = 'S2MSI1C'
                        product_dict['name'] = value['title']
                        product_dict['uuid'] = key

                        product_dict['size'] = value['size'][0:-3]

                        product_dict['preview_url'] = value['link_icon']
                        product_dict['manual_product_url'] = value['link']
                        product_dict['manual_download_url'] = value['link_alternative']
                        product_dict['manual_bulkorder_url'] = None
                        # TODO: create a link to teh metaddata files using http get request
                        product_dict['metadata_url'] = None
                        product_dict['last_modified'] = value['ingestiondate']
                        product_dict['bulk_inprogress'] = None
                        product_dict['summary'] = value['summary']
                        product_dict['sat_name'] = value['platformserialidentifier']
                        product_dict['vendor_name'] = value['identifier']

                        # TODO: write a conversion module for converting between pathrow and MGRS centroids (nearest neighbor or most coverage)
                        product_dict['pathrow'] = None

                        # TODO: calculate this value once the atmos and scene classes are done
                        product_dict['land_cloud_percent'] = None

                        product_dict['cloud_percent'] = value['cloudcoverpercentage']

                        product_dict['platform_name'] = value['platformname']
                        product_dict['instrument'] = value['instrumentshortname']

                        # TODO: Create a converter that converts PATH/ROW to MGRS and vice Versa
                        if 'tileid' in value.keys():
                            product_dict['mgrs'] = value['tileid']
                        else:
                            product_dict['mgrs'] = 'n/a'
                        product_dict['orbit'] = value['relativeorbitnumber']
                        product_dict['abs_orbit'] = value['orbitnumber']

                        products_dict[key] = product_dict

            return products_dict
        else:
            print('No product found.')
            return {}
