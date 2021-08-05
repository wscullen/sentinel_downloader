from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt
import os
import json
import datetime
from datetime import datetime as dt
import requests
from requests.auth import HTTPBasicAuth

import collections

from .transfer_monitor import TransferMonitor

from .utils import TaskStatus, ConfigFileProblem, ConfigValueMissing

from collections import OrderedDict
from lxml import etree
from pathlib import Path

import logging
import yaml


class S2Downloader:
    def __init__(self, path_to_config="config.yaml", username=None, password=None):

        # create logger
        self.logger = logging.getLogger(__name__)

        # create console handler and set level to debug
        # ch = logging.StreamHandler()
        # ch.setLevel(logging.DEBUG)
        # formatter = logging.Formatter(
        #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        # )
        # ch.setFormatter(formatter)
        # self.logger.addHandler(ch)
        # self.logger.propagate = False

        # Load config from config.yaml
        try:
            with open(path_to_config, "r") as stream:
                config = yaml.safe_load(stream)

        except yaml.YAMLError as exc:
            self.logger.error("Problem loading config... exiting...")
            raise ConfigFileProblem

        except FileNotFoundError as e:
            self.logger.error(f"Missing config file with path {path_to_config}")
            raise e

        except BaseException as e:
            self.logger.error("Unknown problem occurred while loading config")

        required_config_keys = [
            "ESA_SCIHUB_USER",
            "ESA_SCIHUB_PASS",
        ]

        self.logger.debug(config.keys())

        try:
            config.keys()
        except AttributeError as e:
            raise ConfigFileProblem

        # Find the difference between sets
        # required_config_keys can be a sub set of config.keys()
        missing_keys = set(required_config_keys) - set(list(config.keys()))

        if len(list(missing_keys)) != 0:
            self.logger.error(
                f"Config file loaded but missing critical vars, {missing_keys}"
            )
            raise ConfigValueMissing

        self.username = config["ESA_SCIHUB_USER"]
        self.password = config["ESA_SCIHUB_PASS"]

        if not (bool(self.username) and bool(self.password)):
            self.logger.error("Missing auth env vars, MISSING USERNAME OR PASSWORD")
            raise ConfigValueMissing

        self.copernicus_url = "https://scihub.copernicus.eu/dhus"

        self.api = SentinelAPI(
            self.username,
            self.password,
            "https://scihub.copernicus.eu/dhus",
            show_progressbars=True,
        )

    def __del__(self):
        pass

    def get_product_info(self, product_id, full=True):
        product_data = self.api.get_product_odata(product_id, full=full)
        return product_data

    def search_for_products(
        self, dataset_name, polygon, query_dict, just_entity_ids=False
    ):
        self.logger.info(f"Searching for products using {query_dict}")
        producttype = None
        filename = None
        sensormode = None

        if "producttype" in query_dict.keys():
            producttype = query_dict["producttype"]

        if "filename" in query_dict.keys():
            filename = query_dict["filename"]

        if "sensoroperationalmode" in query_dict.keys():
            sensormode = query_dict["sensoroperationalmode"]

        self.logger.info(
            f"product type: {producttype}, filename: {filename}, sensormode: {sensormode}"
        )

        results = self.api.query(
            area=polygon,
            filename=filename,
            producttype=producttype,
            sensoroperationalmode=sensormode,
            date=query_dict["date"],
            area_relation="Intersects",
            platformname=dataset_name,
        )
        self.logger.info(f"Query results: {results}")

        return results

    def search_for_products_by_name(
        self, dataset_name, names, query_dict, just_entity_ids=False
    ):
        self.logger.info(f"Searching for products by name using query {query_dict}")
        producttype = None
        filename = None
        sensormode = None

        if "producttype" in query_dict.keys():
            producttype = query_dict["producttype"]

        if "filename" in query_dict.keys():
            filename = query_dict["filename"]

        if "sensoroperationalmode" in query_dict.keys():
            sensormode = query_dict["sensoroperationalmode"]

        self.logger.info(
            f"product type: {producttype}, filename: {filename}, sensormode: {sensormode}"
        )

        names_formatted_for_search = []
        for name in names:
            if name[:3] == "L1C":
                name_parts = name.split("_")
                usgs_name = f"*S2*_MSIL1C_{name_parts[3][:8]}*{name_parts[1]}*"
                names_formatted_for_search.append(f"(filename:{usgs_name})")
            else:
                names_formatted_for_search.append(f"(filename:{name}*)")

        names_raw_query_str = " or ".join(names_formatted_for_search)

        self.logger.info(f"Raw query string: {names_raw_query_str}")
        self.logger.info(f"Dataset name: {dataset_name}")

        results = collections.OrderedDict([])
        for name in names:
            result = self.api.query(raw=name)
            results.update(result)

        self.logger.info(f"Query results: {results}")

        return results

    def search_for_products_by_tile(
        self, tiles, date_range, just_entity_ids=False, product_type=None
    ):

        products = OrderedDict([])

        query_kwargs = {
            "platformname": "Sentinel-2",
            "date": (date_range[0], date_range[1]),
        }

        # S2MSI1C, S2MS2Ap
        if product_type == "L1C":
            query_kwargs["producttype"] = "S2MSI1C"
        elif product_type == "L2A":
            query_kwargs["producttype"] = "S2MSI2A"

        for tile in tiles:
            kw = query_kwargs.copy()
            kw["filename"] = f"*_T{tile}_*"  # products after 2017-03-31
            pp = self.api.query(**kw)
            products.update(pp)

        for prod in products:
            products[prod]["api_source"] = "esa_scihub"

        self.logger.info(f"Products found when searching by tile: {products}")

        return products

    def search_for_products_by_footprint(self, wkt, date_range, product_type=None):
        products = OrderedDict([])

        query_kwargs = {
            "footprint": wkt,
            "platformname": "Sentinel-2",
            "beginposition": f'[{date_range[0]} TO {date_range[1]}]',
            "beginposition": f'[{date_range[0]} TO {date_range[1]}]'
        }

        raw = f'(footprint:"Intersects({wkt})") AND ( beginPosition:[{date_range[0]} TO {date_range[1]}] AND endPosition:[{date_range[0]} TO {date_range[1]}] ) AND ( (platformname:Sentinel-2))'

        # S2MSI1C, S2MS2Ap
        if product_type == "L1C":
            query_kwargs["producttype"] = "S2MSI1C"
        elif product_type == "L2A":
            query_kwargs["producttype"] = "S2MSI2A"

        products = self.api.query(raw=raw)

        for prod in products:
            products[prod]["api_source"] = "esa_scihub"

        self.logger.info(f"Products found when searching by tile: {products}")

        return products

    def get_esa_product_name(
        self, platformname, relative_orbit_number, filename_query, sensingdate
    ):
        """Use this function to get the correct product name from the ESA"""
        # (platformname:Sentinel-2 AND relativeorbitnumber:41 AND filename:*T13UGS* AND beginPosition:[2018-10-22T00:00:00.000Z TO 2018-10-23T00:00:00.000Z])
        end_date = sensingdate + datetime.timedelta(days=1)
        date_tuple = (sensingdate.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))

        results = self.api.query(
            date=date_tuple,
            platformname=platformname,
            relativeorbitnumber=relative_orbit_number,
            filename=filename_query,
        )

        self.logger.info(f"ESA Product name results: {results}")

        for result in results:
            result["api_source"] = "esa_scihub"

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

    def build_download_url(self, tile_id):
        url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{tile_id}')/Nodes"
        # https://scihub.copernicus.eu/dhus/odata/v1/Products('a8f318d3-b95f-44f6-aa7e-bccbe4b00c4f')/
        # Nodes('S2B_MSIL1C_20190628T182929_N0207_R027_T12UUA_20190628T221748.SAFE')/
        # Nodes('GRANULE')/
        # Nodes('L1C_T12UUA_A012065_20190628T183312')/
        # Nodes('IMG_DATA')/Nodes
        r = requests.get(url=url, auth=(self.username, self.password), timeout=2 * 60.0)
        self.logger.info(
            f"Status code: {r.status_code}, content: {r.content}, text: {r.text}"
        )

        XHTML_NAMESPACE = "http://www.w3.org/2005/Atom"
        XHTML = "{%s}" % XHTML_NAMESPACE

        NSMAP = {None: XHTML_NAMESPACE}  # the default namespace (no prefix)

        # xhtml = etree.Element(XHTML + "html", nsmap=NSMAP)  #
        xml = r.text.encode("utf-8")
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        h = etree.fromstring(xml, parser=parser)

        result = h.find("entry/id", h.nsmap)
        self.logger.debug(f"xml text: {result.text}")

        product_name = result.text.split("/")[-1][7:-2]
        self.logger.debug(f"Product name: {product_name}")

        next_url = f"{result.text}/Nodes('GRANULE')/Nodes"

        next_r = requests.get(
            url=next_url, auth=(self.username, self.password), timeout=2 * 60
        )

        xml = next_r.text.encode("utf-8")
        h = etree.fromstring(xml, parser=parser)
        self.logger.debug(f"Next result text: {next_r.text}")
        result = h.find("entry/id", h.nsmap)
        self.logger.debug(f"Next result text: {result.text}")

        granule_name = result.text.split("/")[-1][7:-2]
        self.logger.info(f"Granule name: {granule_name}")
        # 'T12UXA_20190620T181921_TCI.jp2' L1C_T12UXA_A020859_20190620T182912  S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306.SAFE
        tci_name = f"{granule_name.split('_')[1]}_{product_name.split('_')[2]}_TCI.jp2"
        next_url = f"{result.text}/Nodes('IMG_DATA')/Nodes('{tci_name}')/$value"

        return next_url

    def download_tci(self, tile_id, directory):

        url = self.build_download_url(tile_id)
        self.logger.info(f"Url created: {url}")
        file_name = url.split("/")[-2][7:-2]
        self.logger.info(f"Downloading true color preview image for: {file_name}")

        full_file_path = Path(directory, file_name)

        r = requests.get(
            url=url, auth=(self.username, self.password), stream=True, timeout=2 * 60
        )

        self.logger.info(f"Response status code: {r.status_code}")

        if not os.path.isfile(full_file_path):
            try:

                transfer = TransferMonitor(full_file_path, 1)
                with open(full_file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1000000):
                        f.write(chunk)

            except BaseException as e:
                transfer.finish()
                return TaskStatus(
                    False, "An exception occured while trying to download.", e
                )
            else:
                transfer.finish()
                return TaskStatus(True, "Download successful", full_file_path)
        else:
            return TaskStatus(
                False, "Requested file to download already exists.", full_file_path
            )

    def download_fullproduct(self, tile_id, tile_name, directory):

        url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{tile_id}')/$value"

        full_file_path = Path(directory, tile_name + ".zip")
        self.logger.info(f"Url created: {url}")

        self.logger.info(f"Downloading full product for {tile_name}")

        try:
            r = requests.get(
                url=url, auth=(self.username, self.password), stream=True, timeout=120.0
            )
        except BaseException as e:
            self.logger.error(e)
            return TaskStatus(
                False, "An exception occured while trying to download.", e
            )

        self.logger.debug(f"Response status code: {r.status_code}")

        if not os.path.isfile(full_file_path):
            try:

                transfer = TransferMonitor(full_file_path, 1)
                with open(full_file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1000000):
                        f.write(chunk)

            except BaseException as e:
                transfer.finish()
                return TaskStatus(
                    False, "An exception occured while trying to download.", e
                )
            else:
                transfer.finish()
                return TaskStatus(True, "Download successful", str(full_file_path))
        else:
            return TaskStatus(
                True, "Requested file to download already exists.", str(full_file_path)
            )

    def download_fullproduct_callback(
        self, tile_id, tile_name, directory, callback=None
    ):
        """ Same as download_fullproduct, except that is supports a callback of the
            form func(current_amount_downloaded_in_bytes, total_amount_to_transfer_in_bytes, percentange_complete)

            For every chunk of the file downloaded, the callback is called.
        
        """
        url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{tile_id}')/$value"

        full_file_path = Path(directory, tile_name + ".zip")

        self.logger.info(f"Url created: {url}")
        self.logger.info(f"Downloading full product for {tile_name}")
        self.logger.info(f"Full file path: {full_file_path}")

        try:
            r = requests.get(
                url=url,
                auth=(self.username, self.password),
                stream=True,
                timeout=2 * 60.0,
            )
        except BaseException as e:
            self.logger.error(e)
            return TaskStatus(
                False, "An exception occured while trying to download.", e
            )
        else:
            self.logger.debug(f"Response status code: {r.status_code}")

            file_size = int(r.headers["Content-Length"])
            transfer_progress = 0
            chunk_size = 10000

            previous_update = 0
            update_throttle_threshold = 1  # Update every percent change

            if not os.path.isfile(full_file_path):
                try:
                    with open(full_file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            transfer_progress += chunk_size
                            transfer_percent = round(
                                min(100, (transfer_progress / file_size) * 100), 2
                            )
                            self.logger.debug(
                                f"Progress: {transfer_progress},  {transfer_percent:.2f}%"
                            )
                            if (
                                transfer_percent - previous_update
                            ) > update_throttle_threshold:
                                callback(transfer_progress, file_size, transfer_percent)
                                previous_update = transfer_percent

                except BaseException as e:
                    return TaskStatus(
                        False, "An exception occured while trying to download.", e
                    )
                else:
                    return TaskStatus(True, "Download successful", str(full_file_path))
            else:
                return TaskStatus(
                    True,
                    "Requested file to download already exists.",
                    str(full_file_path),
                )

    def request_offline_product(self, tile_id):
        """ For products with the Offline status, it is a regular download request
            with no intention of downloading the file, if the response is 202, the
            request to retrieve the product was received, and the product's Online
            property should be checked periodically to determine when it should be
            actually downloaded.
        
        """
        url = f"https://scihub.copernicus.eu/dhus/odata/v1/Products('{tile_id}')/$value"
        # https://scihub.copernicus.eu/dhus/odata/v1/Products('bd22f901-a796-4553-acc9-73cbc34e0f40')/$value
        # https://scihub.copernicus.eu/dhus/odata/v1/Products('c94ffc0b-62e5-4f76-83b8-e7f0e5a7542e')/$value
        # https://scihub.copernicus.eu/dhus/odata/v1/Products(' Id ')/$value
        self.logger.info(f"Url created: {url}")
        self.logger.info(self.username)
        self.logger.info(self.password)
        try:
            r = requests.get(
                url=url,
                auth=(self.username, self.password),
                stream=False,
                timeout=2 * 60,
            )
        except BaseException as e:
            self.logger.error(e)
            return False
        else:
            self.logger.debug(f"Response status code: {r.status_code}")

            if r.status_code == 202:
                self.logger.debug(
                    "Request to move product to Online state was successful"
                )
                return True
            else:
                self.logger.debug(
                    "Request to move product to Online state either failed or encountered unknown behaviour"
                )
                return False

    def download_file(self, url, download_name, download_id):
        """Download from scihub using requests library and their api.
        URL of the form:

        https://scihub.copernicus.eu/dhus/odata/v1/Products('2b17b57d-fff4-4645-b539-91f305c27c69')/$value

        if the "api_source" is "usgs_ee", then the esa uuid will have to be found before downloading
        Maximum concurrent downloads is 2.
        """

        self.logger.info(f"Downloading file for url {url}")

        r = requests.get(
            url=url, auth=(self.username, self.password), stream=True, timeout=2 * 60.0
        )

        self.logger.debug(f"Response status code: {r.status_code}")

        if not os.path.isfile(download_name):
            try:

                transfer = TransferMonitor(download_name, download_id)
                with open(download_name, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1000000):
                        f.write(chunk)

            except BaseException as e:
                transfer.finish()
                return TaskStatus(
                    False, "An exception occured while trying to download.", e
                )
            else:
                transfer.finish()
                return TaskStatus(True, "Download successful", download_name)
        else:
            return TaskStatus(
                False, "Requested file to download already exists.", download_name
            )

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

    def search_for_products_by_tile_directly(self, tile, daterange):
        """
                    #         producttype:	Used to perform a search based on the product type.
            # Syntax:
            # producttype:<producttype>

            # Possible values for for <producttype> are the following, listed per mission:

            # Sentinel-1: SLC, GRD, OCN
            # Sentinel-2: S2MSI1C, S2MS2Ap
            # Sentinel-3: SR_1_SRA___, SR_1_SRA_A, SR_1_SRA_BS, SR_2_LAN___, OL_1_EFR___, OL_1_ERR___, OL_2_LFR___, OL_2_LRR___, SL_1_RBT___, SL_2_LST___, SY_2_SYN___, SY_2_V10___, SY_2_VG1___, SY_2_VGP___.
     
            # time in yyyy-MM-ddThh:mm:ss.SSSZ (ISO8601 format)
        """

        self.logger.info("Searching for product by tile directly...")

        date_start = dt.strptime(daterange[0], "%Y%m%d")
        date_start.replace(tzinfo=datetime.timezone.utc)

        date_end = dt.strptime(daterange[1], "%Y%m%d")
        date_end.replace(tzinfo=datetime.timezone.utc)

        date_start_string = date_start.isoformat(timespec="milliseconds") + "Z"
        date_end_string = date_end.isoformat(timespec="milliseconds") + "Z"

        date_query = f"beginposition:[{date_start_string} TO {date_end_string}]"
        platform_query = "platformname:Sentinel-2"
        filename_query = f"filename:*_T{tile}_*"

        query_url = f"https://scihub.copernicus.eu/dhus/search?q=({date_query} AND {platform_query} AND {filename_query})"

        r = requests.get(
            query_url,
            auth=HTTPBasicAuth(self.username, self.password),
            timeout=2 * 60.0,
        )

        self.logger.debug(
            f"Response code: {r.status_code}, content: {r.content}, headers: {r.headers}"
        )

        # if r.status_code == 200:
        #     result = r.json()
        #     return result
