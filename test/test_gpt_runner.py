import unittest
from pathlib import Path
import os

import gpt_runner
import datetime


TEST_DIR = os.path.dirname(os.path.abspath(__file__))

class TestGPTRunner(unittest.TestCase):

    def setUp(self):
        self.product_path_arg = '/home/cullens/Development/s2d2/temp/S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB.SAFE'
        self.target_path_arg = '/home/cullens/Development/s2d2/temp'
        self.path_to_graph_xml = '/home/cullens/Development/sentinel_downloader/gpt_graphs/s1/terrain_flattening_ver1_vars.xml'


        self.arg_dict = {
            'filterSize': 11,
            'bitDepth': 'float32'
        }

    def test_initial_variable_creation(self):

        runner = gpt_runner.GPTRunner(self.product_path_arg,
                                      self.target_path_arg,
                                      self.path_to_graph_xml,
                                      self.arg_dict,
                                      1)

        self.assertEquals(runner.product_name, 'S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB')

    def test_properties_file_creation(self):

        runner = gpt_runner.GPTRunner(self.product_path_arg,
                                      self.target_path_arg,
                                      self.path_to_graph_xml,
                                      self.arg_dict,
                                      1)

        runner.generate_properties_file()

        self.assertTrue(Path('./process1.properties').is_file())


    def test_run_processing(self):
        runner = gpt_runner.GPTRunner(self.product_path_arg,
                                      self.target_path_arg,
                                      self.path_to_graph_xml,
                                      self.arg_dict,
                                      1)

        runner.generate_properties_file()
        date_start = datetime.datetime.now()
        runner.run_graph()
        date_end = datetime.datetime.now()

        time_elapsed = date_end - date_start
        d = {}
        d["hours"], rem = divmod(time_elapsed.seconds, 3600)
        d["minutes"], d["seconds"] = divmod(rem, 60)
        print(d)


    # def test_generate_properties_file(self):
    #     results = self.downloader_obj.search_for_products_polygon_to_tiles(self.SENTINEL2_DATASET_NAME,
    #                                                             self.test_footprint_1,
    #                                                             self.QUERY_DICT_EXAMPLE,
    #                                                             detailed=True)

    #     print(len(results))
    #     self.assertTrue(True)

    # def test_search_for_products_polygon_to_tiles_abagextent(self):
    #     results = self.downloader_obj.search_for_products_polygon_to_tiles(self.SENTINEL2_DATASET_NAME,
    #                                                             self.test_footprint_1,
    #                                                             self.QUERY_DICT_EXAMPLE,
    #                                                             detailed=True)

    #     print(len(results))
    #     self.assertTrue(True)

    # def test_search_for_products_polygon_to_tiles_sentinel2(self):
    #     results = self.downloader_obj.search_for_products(self.SENTINEL2_DATASET_NAME,
    #                                                       self.test_footprint_3_lethbridge,
    #                                                       self.QUERY_DICT_EXAMPLE)

    #     print(len(results))
    #     print(results[0])
    #     print(results)
    #     single_element_list = []
    #     single_element_list.append(results[0])
    #     print(single_element_list)
    #     # populated_result = self.downloader_obj.populate_result_list(single_element_list,
    #     #                                                             self.SENTINEL2_PLATFORM_NAME,
    #     #                                                             self.SENTINEL2_DATASET_NAME)
    #     # print(populated_result)
    #     self.assertTrue(True)

    # @timeit
    # def test_get_dataset_metadata_info_sentinel2(self):

    #     results = self.downloader_obj.get_dataset_field_ids(self.SENTINEL2_DATASET_NAME)

    #     # print(results)
    #     self.assertTrue(True)

    # @timeit
    # def test_populate_result_list(self):
    #     # Load previous results
    #     json_results = None
    #     with open(self.path_to_intermediate_query_results_usgs_ee, 'r') as json_file:
    #         json_results = json.load(json_file)

    #     print(json_results)

    #     cleaned_results = self.downloader_obj.populate_result_list(json_results, self.SENTINEL2_PLATFORM_NAME, self.SENTINEL2_DATASET_NAME, detailed=False)

    #     cleaned_results_compare = None
    #     with open(self.path_to_cleaned_query_results_usgs_ee, 'r') as outfile:
    #         clean_results_compare = json.load(outfile)

    #     self.assertEqual(clean_results_compare, cleaned_results)

    # @timeit
    # def test_search_scene_metadata(self):
    #     json_results = None
    #     with open(self.path_to_intermediate_query_results_usgs_ee, 'r') as json_file:
    #         json_results = json.load(json_file)

    #     data_results = json_results['data']['results']
    #     just_entity_ids = [d['entityId'] for d in data_results]

    #     print(just_entity_ids)

    #     detailed_metadata_results = self.downloader_obj.search_scene_metadata(self.SENTINEL2_DATASET_NAME,
    #                                                                           just_entity_ids)

    #     print(detailed_metadata_results)
    #     self.assertEqual(len(detailed_metadata_results), 12)

    # @timeit
    # def test_search_for_products_by_tile(self):
    #     results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
    #                                                     self.tile_list_small,
    #                                                     self.QUERY_DICT_EXAMPLE,
    #                                                     detailed=True)

    #     print(results)
    #     self.assertEqual(len(results), 12)

    # def test_alberta_ag_extent(self):

    #     results = self.downloader_obj.search_for_products_polygon_to_tiles(
    #                 self.SENTINEL2_DATASET_NAME,
    #                 self.test_footprint_2,
    #                 self.QUERY_DICT_EXAMPLE,
    #                 detailed=True)

    #     self.assertEqual(len(results), 988)

    # @timeit
    # def test_search_for_products_by_tile_detailed(self):
    #     results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
    #                                                     self.tile_list_small,
    #                                                     self.query_dict_example_single_tile,
    #                                                     detailed=True)

    #     print(results)
    #     self.assertEqual(len(results), 1)

    # @timeit
    # def test_search_for_products_by_tile_not_detailed(self):
    #     results = self.downloader_obj.search_for_products_by_tile(self.SENTINEL2_DATASET_NAME,
    #                                                     self.tile_list_small,
    #                                                     self.query_dict_example_single_tile,
    #                                                     detailed=False)

    #     print(results)
    #     self.assertEqual(len(results), 1)

    # # def test_find_mgrs_intersection_coarse(self):
    # #     result_list = utilities.find_mgrs_intersection_large(self.test_footprint_2)

    # #     self.assertEqual(set(result_list), set(self.test_footprint_2_result_list))

    # # def test_find_mgrs_intersection_fine_single(self):

    # #     single_gzd = '12U'

    # #     fine_result_list_single = utilities.find_mgrs_intersection_100km_single(self.test_footprint_2,
    # #                                                                             single_gzd)

    # #     self.assertEqual(set(fine_result_list_single), set(self.test_footprint_2_result_list_single))

    # # def test_find_mgrs_intersection_fine(self):

    # #     gzd_initial_list = self.test_footprint_2_result_list

    # #     fine_result_list = utilities.find_mgrs_intersection_100km(self.test_footprint_2,
    # #                                                               gzd_initial_list)

    # #     self.assertEqual(set(fine_result_list), set(self.test_footprint_2_result_list2))


if __name__ == '__main__':
    unittest.main()