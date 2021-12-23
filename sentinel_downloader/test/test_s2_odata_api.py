import unittest
import os
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

from .. import s2_odata_api


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print(BASE_DIR)


class TestS2OdataApi(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    def test_create_url(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        check_online_url = s2_odata.create_url(
            "f5e8573d-d16a-4dcd-861c-e3f6ff32d71f", s2_odata_api.UrlTypes.ONLINE
        )

        self.assertEqual(
            check_online_url,
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('f5e8573d-d16a-4dcd-861c-e3f6ff32d71f')/Online/$value",
        )

    def test_is_product_online(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        online = s2_odata.is_product_online("2bdaf619-f9e3-45cd-a5bf-bbb448c1b672")

        self.assertTrue(online)

    def test_download_product(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        def callback(progress: int, file_size: int, percent: float):
            logging.debug(f"Progress: {progress}")
            logging.debug(f"Percent: {percent}")

        result = s2_odata.download_product("f5e8573d-d16a-4dcd-861c-e3f6ff32d71f")

        self.assertEqual(
            result, "S2B_MSIL2A_20210607T182919_N0300_R027_T11UQQ_20210607T230730.zip"
        )

    def test_download_quicklook(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        result = s2_odata.download_quicklook("f5e8573d-d16a-4dcd-861c-e3f6ff32d71f")

        self.assertEqual(
            result,
            "S2B_MSIL2A_20210607T182919_N0300_R027_T11UQQ_20210607T230730-ql.jpg",
        )

    def test_get_product_name(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        product_name = s2_odata.get_product_name("f5e8573d-d16a-4dcd-861c-e3f6ff32d71f")

        logging.debug(product_name)

        self.assertEqual(
            product_name,
            "S2B_MSIL2A_20210607T182919_N0300_R027_T11UQQ_20210607T230730.SAFE",
        )

    def test_download_manifest(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        product_name = s2_odata.get_product_name("f5e8573d-d16a-4dcd-861c-e3f6ff32d71f")

        logging.debug(product_name)

        manifest_name = s2_odata.download_manifest(
            "f5e8573d-d16a-4dcd-861c-e3f6ff32d71f", product_name
        )

        self.assertEqual(
            product_name,
            "S2B_MSIL2A_20210607T182919_N0300_R027_T11UQQ_20210607T230730.SAFE",
        )

        self.assertEqual(manifest_name, "manifest.safe")

    def test_download_high_resolution_tci(self):
        s2_odata = s2_odata_api.S2OdataHelper(
            username="skullen", password="M0n796St3Ruleth4"
        )

        product_name = s2_odata.get_product_name("f5e8573d-d16a-4dcd-861c-e3f6ff32d71f")

        s2_odata.download_high_resolution_tci(
            "f5e8573d-d16a-4dcd-861c-e3f6ff32d71f", None, product_name
        )


if __name__ == "__main__":
    unittest.main()
