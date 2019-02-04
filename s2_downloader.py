from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import os
import json
import datetime
import requests

import collections

from transfer_monitor import TransferMonitor

from utils import TaskStatus

class S2Downloader():
    def __init__(self, config_path):
        self.config_path = config_path
        self.copernicus_url = 'https://scihub.copernicus.eu/dhus'

        if os.path.exists(self.config_path):
            with open(self.config_path) as f:
                self.config = json.load(f)
        else:
            self.config = None

            raise Exception

        self.username = self.config['SENTINEL_USER']
        self.password = self.config['SENTINEL_PASS']

        self.api = SentinelAPI(self.username,
                               self.password,
                               'https://scihub.copernicus.eu/dhus',
                               show_progressbars=True)

    def search_for_products(self, dataset_name, polygon, query_dict, just_entity_ids=False):
        print(query_dict)
        producttype = None
        filename = None
        sensormode = None


        if 'producttype' in query_dict.keys():
            producttype = query_dict['producttype']

        if 'filename' in query_dict.keys():
            filename = query_dict['filename']

        if 'sensoroperationalmode' in query_dict.keys():
            sensormode = query_dict['sensoroperationalmode']


        print(producttype, filename, sensormode)


        results = self.api.query(area=polygon,
                                 filename=filename,
                                 producttype=producttype,
                                 sensoroperationalmode=sensormode,
                                 date=query_dict['date'],
                                 area_relation='Intersects',
                                 platformname=dataset_name,)
        print(results)
        return results

    def search_for_products_by_name(self, dataset_name, names, query_dict, just_entity_ids=False):

        print(query_dict)
        producttype = None
        filename = None
        sensormode = None


        if 'producttype' in query_dict.keys():
            producttype = query_dict['producttype']

        if 'filename' in query_dict.keys():
            filename = query_dict['filename']

        if 'sensoroperationalmode' in query_dict.keys():
            sensormode = query_dict['sensoroperationalmode']

        names_formatted_for_search = []
        for name in names:
            names_formatted_for_search.append(f'(filename:{name}*)')

        names_raw_query_str = " or ".join(names_formatted_for_search)

        print(names_raw_query_str)
        print(dataset_name)

        print(producttype, filename, sensormode)
        results = collections.OrderedDict([])
        for name in names:

            result = self.api.query(filename=name + "*",
                                    platformname=dataset_name)
            results.update(result)

        print(results)
        return results


    def get_esa_product_name(self, platformname, relative_orbit_number, filename_query, sensingdate):
        """Use this function to get the correct product name from the ESA"""
        # (platformname:Sentinel-2 AND relativeorbitnumber:41 AND filename:*T13UGS* AND beginPosition:[2018-10-22T00:00:00.000Z TO 2018-10-23T00:00:00.000Z])
        end_date = sensingdate + datetime.timedelta(days=1)
        date_tuple = (sensingdate.strftime('%Y%m%d'),
                      end_date.strftime('%Y%m%d'))

        results = self.api.query(date=date_tuple,
                                 platformname=platformname,
                                 relativeorbitnumber=relative_orbit_number,
                                 filename=filename_query)

        print(results)

        return results

    def download_product(self, product, folder):
        """ Uses ``sentinelhub`` to download from AWS S3 Bucket

        This is a simple wrapper function that utilizes the ``sentinelhub`` \
        library to download S2 data from AWS. If an exception is encountered, \
        the function returns a failed status (str) which is used in the job log \
        file.

        Args:
            product_id (str): Title of the product to download. It is looking for \
            a product title in the form: \
            ``S2A_MSIL1C_20170404T183821_N0204_R027_T12UVF_20170404T183816``

            folder (str): Name of the relative folder to store the .SAFE download \
            in. Should use TEMP_DIR, but '.' is a valid option as well.

        Returns:
            str: "success" if everything works, "failed (Exception details)" \
                if it does not work.
        """

        print(product)
        print(folder)

        # product_id = product['name']

        # download_result = None

        # s1_bucket = 'sentinel-s1-l1c'
        # s2_bucket = 'sentinel-s2-l1c'

        # tqdm.tqdm.write('Trying to download {} from AWS...'.format(product_id))

        # # with HiddenPrints():
        # try:
        #     # download_result = sentinelhub.download_safe_format(
        #     #     product_id=product_id,
        #     #     folder='./' + folder,
        #     #     threaded_download=True, )

        #     # product_id = 'S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
        #     if product_type == 'Sentinel-2':
        #         # BUG: New version of SentinelHub DOES NOT download products prior to Dec 2016
        #         # Despite their docs saying it does
        #         print('Downloding sentinel 2 products from AWS.... \n')

        #         product_request = sentinelhub.AwsProductRequest(
        #             product_id=product_id, data_folder=folder, safe_format=True)
        #         download_result = product_request.save_data()

        #     elif product_type == 'Sentinel-1':
        #         # NOTE: using Alaska Facility Downloads instead of AWS s3 for
        #         # the time being

        #         print('Downloding sentinel 1 products from ASF.... \n')
        #         # boto low level s3 request goes here
        #         # logger.info('Sentinel-1 Product Requested, need to do an actual S3 request...')
        #         # download_result = s3_product_request(product_id, s1_bucket )
        #         download_result = asf_download_zip(product, folder, auth_dict)

        # except Exception as e:
        #     logger.debug(
        #         'Something went wrong with this download. {}'.format(e))
        #     tqdm.tqdm.write(
        #         'Sorry something went wrong when trying to download, {}'.format(e))

        #     return 'failed ({})'.format(str(e))

        # logger.debug('Download result: {}'.format(download_result))

        # return 'success'
        pass

    def download_file(self, url, download_name, download_id):
        """Download from scihub using requests library and their api.
        URL of the form:

        https://scihub.copernicus.eu/dhus/odata/v1/Products('2b17b57d-fff4-4645-b539-91f305c27c69')/$value

        if the "api_source" is "usgs_ee", then the esa uuid will have to be found before downloading
        Maximum concurrent downloads is 2.
        """

        r = requests.get(url=url, auth=(self.username, self.password), stream=True)

        print(r.status_code)

        if not os.path.isfile(download_name):
            try:

                transfer = TransferMonitor(download_name, download_id)
                with open(download_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1000000):
                        f.write(chunk)
                transfer.finish()

            except BaseException as e:
                return TaskStatus(False, 'An exception occured while trying to download.', e)
            else:
                return TaskStatus(True, 'Download successful', download_name)
        else:
            return TaskStatus(False, 'Requested file to download already exists.', download_name)


        pass

    def s3_product_request(self, product_id, bucket_id):
        """ NOT IMPLEMENTED

            Uses the boto3 aws sdk to query the sentinel1 aws bucket
            [product type]/[year]/[month]/[day]/[mode]/[polarization]/[product identifier]

            For example, the files for individual scene are available in the following location:
            s3://sentinel-s1-l1c/GRD/2018/1/11/EW/DH/S1A_EW_GRDH_1SDH_20180111T110409_20180111T110513_020106_02247C_FBAB/

            product_id format is S1A_EW_GRDH_1SDH_20180111T110409_20180111T110513_020106_02247C_FBAB

            Where:

            [product type] = GRD - GRD or SLC

            [year] = e.g. 2018 - is the year the data was collected.

            [month] = e.g. 1 - is the month of the year the data was collected (without leading zeros).

            [day] = e.g. 11 - is the day of the month the data was collected (without leading zeros).

            [mode] = e.g. EW - IW, EW, WV, IW1-IW3, EW1-EW5, WV1-WV2, S1-S6, IS1-IS7; note that modes depend on the [product type]

            [polarization] = e.g. DH - DH, DV, SH, SV, VH, HV, HV, VH; note that polarizations depend on the observation scenario

            product identifier - original product identifier
        """
        # sample product id
        # S1A_EW_GRDH_1SDH_20180111T110409_20180111T110513_020106_02247C_FBAB

        # product_type = product_id[7:11]
        # year = product_id[20:24]
        # month = product_id[24:26]
        # day = product_id[26:28]
        # mode = product_id[4:6]
        # polarization = product_id[14:16]

        # logger.info('incoming S3 request params: product_type={}\nyear={}\nmonth={}'
        #             '\nday={}\nmode={}\npolarization={}'.format(product_type,
        #                                                         year,
        #                                                         month,
        #                                                         day,
        #                                                         mode,
        #                                                         polarization))

        # # NOTE: Not yet implemented with boto library

        # return 'Failure'
        pass
