import requests
import logging
import os
import json
from typing import Dict, Tuple, List, Optional

from pathlib import Path
import zipfile


from utils import TaskStatus

import sentinel_downloader.s2_downloader as esa_downloader


class S1Downloader():
    def __init__(self, path_to_config):

        self.config_path = path_to_config

        if os.path.exists(self.config_path):
            with open(self.config_path) as f:
                self.config = json.load(f)
        else:
            self.config = None

            raise Exception

        # TODO Should change this to use Env vars
        self.esa_username = self.config['SENTINEL_USER']
        self.esa_password = self.config['SENTINEL_PASS']

        self.asf_username = self.config['ASF_USER']
        self.asf_password = self.config['ASF_PASS']

        self.primary_dl_src = self.config['S1']['DOWNLOAD']

        self.esa_downloader = esa_downloader.S2Downloader(self.config_path)

        if self.primary_dl_src == 'USGS_ASF':
            self.secondary_dl_src = 'ESA_SCIHUB'
        elif self.primary_dl_src == 'ESA_SCIHUB':
            self.secondary_dl_src = 'USGS_ASF'


    def s1_download_wrapper(self, product: Dict, dest_dir: str) -> TaskStatus:
        """Wraps primary dl funcs depending on configured download source.
        """


        # Check if download already exists
        if Path(dest_dir, product['name'] + '.zip').is_file():
            print(Path(dest_dir, product['name'] + '.zip'))


            return TaskStatus(True, f'Product zip already exists in dest dir {product["name"]}', None)

        print(self.primary_dl_src)

        if self.primary_dl_src == 'USGS_ASF':
            result = self.asf_download_zip(product, dest_dir)

        elif self.primary_dl_src == 'ESA_SCIHUB':
            result = self.esa_downloader.download_product(product, dest_dir)

        return result


    def asf_download_zip(self, product: Dict, download_folder: str) -> TaskStatus:
        """ Uses ASF (Alaska Satellite Facility) to download S1 data products

            The ASF download procedure is very simple: create a URL from the
            product name, create an authenticated HTTP request for the product
            .zip archive.

            Example URL 1
            https://datapool.asf.alaska.edu/ # base url
            GRD_HS/ # product type, GRD, resolution H high, pol, Single
            SB/ # platform, sentinel 1 B
            S1B_IW_GRDH_1SSV_20161014T012841_20161014T012906_002496_00435F_BB18.zip

            # Product name with zip concat to  it

        """

        logger = logging.getLogger(__name__)

        download_baseurl = 'https://datapool.asf.alaska.edu'

        p_type = product['product_type']
        p_format = product['detailed_metadata']['format']
        p_polarization = product['polarization_mode']
        p_sensormode = product['sensor_mode']

        if p_polarization == 'VV' or p_polarization == 'HH':
            polarization = 'S'
        else:
            polarization = 'D'

        if product['name'].find('S1A') != -1:
            platform = 'SA'
        elif product['name'].find('S1B') != -1:
            platform = 'SB'
        else:
            return TaskStatus(False, "FAILED, invalid product title", None)

        if product['name'].find('{}'.format(p_type)) != -1:
            res_index = product['name'].find('{}'.format(p_type))
            resolution = product['name'][res_index + 3:res_index + 4]
        else:
            return TaskStatus(False, "FAILED, invalid product name", None)

        product_name = product['name'] + '.zip'

        if p_type == 'GRD':
            p_type_res_pol = f"{p_type}_{resolution}{polarization}"
        elif p_type == 'SLC':
            p_type_res_pol = f"{p_type}"

        download_url = "{}/{}/{}/{}".format(download_baseurl,
                                                p_type_res_pol,
                                                platform,
                                                product_name)


        USERNAME = self.asf_username
        PASSWORD = self.asf_password

        print(download_url)

        init_resp = requests.get(download_url)
        data_resp = requests.get(init_resp.url, stream=True, auth=(USERNAME, PASSWORD))

        result_status = None

        if data_resp.status_code == 200:
            # Success! we have initialized correctly and can now make a request to
            # the TRUE url, which will allow us to authenticate and download the product

            # Size of file to download and write at a time, bigger chunks = more memory used
            chunk_size = 1024 * 1024

            FILENAME = os.path.join(download_folder, product_name)
            try:
                with open(FILENAME, 'wb') as fd:
                    logger.debug('Starting sentinel1 download...')

                    for chunk in data_resp.iter_content(chunk_size):
                        logger.debug('Writing chunk of file to disk... ')
                        fd.write(chunk)
            except BaseException as e:
                logger.critical('Unknown error occured while trying to download, {}'.format(e))

            logger.debug('Finished s1 download for product {}'.format(product_name))

            result_status = TaskStatus(True, None, None)

        elif data_resp.status_code == 404:
            logger.critical('The supplied product url cannot be found')
            result_status = TaskStatus(False, 'The supplied product URL cannot be found.', None)
        elif data_resp.status_code == 401:
            logger.critical('Problem with authenication')
            result_status = TaskStatus(False, 'Problem with authentication', None)
        else:
            logger.critical('Unkown status code, failure {}'.format(data_resp.status_code))
            result_status = TaskStatus(False, f'Unknown status code ({data_resp.status_code}) failure.', None)

        return result_status

    def validate_zip(self, product_name, path_to_zip):


        path_to_product_zip = Path(path_to_zip, product_name + '.zip')

        try:
            print('trying to extract downloaded archive')

            with zipfile.ZipFile(path_to_product_zip) as zf:

                # zf.extractall(path=extraction_path)
                for zip_info in zf.infolist():
                    print(zip_info.filename)

                    if zip_info.filename[-1] == '/':
                        continue

                    zip_info.filename = zip_info.filename.split('/')[-1]

                    # if actual_file_stem == "":
                    #     actual_file_stem = zip_info.filename.split('.')[0]

                    # # Extract only the files to a specific dir
                    # zf.extract(zip_info, DATA_DIR)


        except zipfile.BadZipFile as e:
            print('Corrupted zip file, deleting, try the '
                            'download again.')
            os.remove(path_to_product_zip)
            dl_status = TaskStatus(False, 'Bad zip file', None)
        except BaseException as e:
            print('Something blew up while unzip, deleting, '
                            'try the download again.')
            print(e)
            dl_status = TaskStatus(False, 'Generic problem while extracting zip', str(e))

        else:

            dl_status = TaskStatus(True, None, path_to_product_zip)

        finally:
            return dl_status