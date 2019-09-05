import unittest

#import s2d2_proj.sentinel_downloader.s2_downloader as s2_downloader

import s2_downloader

import os

from pathlib import Path

# L1C_T12UVA_A012537_20190731T183929
# L1C_T12UVA_A012537_20190731T184118


class TestS2Downloader(unittest.TestCase):
    def setUp(self):
        pass

    def test_build_url(self):
        s2_dl = s2_downloader.S2Downloader("")

        result = s2_dl.build_download_url("5ff875a1-bc41-4c43-adae-be4dfa03ad5f")
        # self.assertTrue(True)

        self.assertEquals(
            result,
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('5ff875a1-bc41-4c43-adae-be4dfa03ad5f')/Nodes('S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306.SAFE')/Nodes('GRANULE')/Nodes('L1C_T12UXA_A020859_20190620T182912')/Nodes('IMG_DATA')/Nodes('T12UXA_20190620T181921_TCI.jp2')/$value",
        )

    def test_download_tci(self):
        s2_dl = s2_downloader.S2Downloader("")

        result = s2_dl.download_tci(
            "6574b5fa-3898-4c9e-9c36-028193764211",
            Path(os.path.abspath(os.path.dirname(__file__)), "testing_data"),
        )
        print(result)
        self.assertTrue(True)

    def test_download_fullproduct(self):
        s2_dl = s2_downloader.S2Downloader("")

        result = s2_dl.download_fullproduct(
            "6574b5fa-3898-4c9e-9c36-028193764211",
            "S2A_MSIL1C_20190620T181921_N0207_R127_T12UXA_20190620T231306",
            Path(os.path.abspath(os.path.dirname(__file__)), "testing_data"),
        )
        print(result)
        self.assertTrue(True)

    def test_search_for_products_by_tile(self):
        """ Test search for products when an L2A product is available """
        # self, tiles, date_range, just_entity_ids=False

        # S2A_MSIL2A_20190904T102021_N0213_R065_T32UPV_20190904T140237

        s2_dl = s2_downloader.S2Downloader("")

        result = s2_dl.search_for_products_by_tile(
            ['32UPV'],
            ('20190904', '20190905'),
        )
        print(result)

    def test_search_for_products_by_tile_directly(self):
        
        s2_dl = s2_downloader.S2Downloader("")

        daterange = ('20190618', '20190619')
        print(daterange)
        result = s2_dl.search_for_products_by_tile_directly('20TMS', daterange)
        
        print(result)


    # def test_properties_file_creation(self):

    #     runner = gpt_runner.GPTRunner(
    #         self.product_path_arg,
    #         self.target_path_arg,
    #         self.path_to_graph_xml,
    #         self.arg_dict,
    #         1,
    #     )

    #     runner.generate_properties_file()

    #     self.assertTrue(Path("./process1.properties").is_file())

    # def test_run_processing(self):
    #     runner = gpt_runner.GPTRunner(
    #         self.product_path_arg,
    #         self.target_path_arg,
    #         self.path_to_graph_xml,
    #         self.arg_dict,
    #         1,
    #     )

    #     runner.generate_properties_file()
    #     date_start = datetime.datetime.now()
    #     runner.run_graph()
    #     date_end = datetime.datetime.now()

    #     time_elapsed = date_end - date_start
    #     d = {}
    #     d["hours"], rem = divmod(time_elapsed.seconds, 3600)
    #     d["minutes"], d["seconds"] = divmod(rem, 60)
    #     print(d)


if __name__ == "__main__":
    unittest.main()
