import logging
import sys
import unittest


from ewoc_s1.cli import generate_s1_ard_from_pids, S1ARDProcessorError, S1DEMProcessorError

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

class Test_Cli(unittest.TestCase):
    def setUp(self):
        logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

    def test_cli_id(self):
        """Products ok"""
        (a,b) =generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20181102T153633_20181102T153700_024411_02ACA1_105A',
                                          'S1A_IW_GRDH_1SDV_20181102T153608_20181102T153633_024411_02ACA1_2B2C'],
                                         '36TWR',dem_source='ewoc',data_source='aws',clean=False)
        print(f'{a} - {b}')

    def test_cli_safe(self):
        """Products with SAFE ID"""
        (a,b)=generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20210708T060040_20210708T060105_038682_04908E_3178.SAFE',
                                         'S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979.SAFE'],
                                        '31TCJ',dem_source='esa',data_source='aws', clean=False)
        print(f'{a} - {b}')

    def test_cli_one_missing_product(self):
        """One product is missing"""
        (a,b)=generate_s1_ard_from_pids(['S1B_IW_GRDH_1SDV_20180325T153530_20180325T153557_010190_012836_761E',
                                         'S1B_IW_GRDH_1SDV_20180325T153530_20180325T153557_010190_012836_4840'],
                                        '36TYR',dem_source='ewoc',data_source='aws')
        print(f'{a} - {b}')

    def test_cli_missing_json(self):
        """Missing `productInfo.json`"""
        with self.assertRaises(S1ARDProcessorError):
            generate_s1_ard_from_pids(['S1B_IW_GRDH_1SDV_20191112T040231_20191112T040256_018889_023A04_C65C'],
                                      '36UVC',dem_source='esa',data_source='aws')

    def test_cli_missing_NRT_product(self):
        """Missing NRT product on aws"""
        with self.assertRaises(S1ARDProcessorError):
            generate_s1_ard_from_pids(['S1B_IW_GRDH_1SDV_20171101T153534_20171101T153600_008090_00E4B2_A2A5'],
                                      '36UVC',dem_source='esa',data_source='aws')

    def test_cli_cop_dem_from_esa(self):
        """test with cop dem retrieve from esa website: tile not covered by srtm"""
        with self.assertRaises(S1DEMProcessorError):
            generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20210310T055600_20210310T055625_036932_045843_9A14',
                                       'S1A_IW_GRDH_1SDV_20210310T055625_20210310T055650_036932_045843_0613'],
                                      '32VKN',dem_source='esa',data_source='aws')

    def test_cli_cop_dem_from_ewoc(self):
        """test with cop dem retrieve from ewoc aux data: tile not covered by srtm """
        with self.assertRaises(S1DEMProcessorError):
            generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20210310T055600_20210310T055625_036932_045843_9A14',
                                       'S1A_IW_GRDH_1SDV_20210310T055625_20210310T055650_036932_045843_0613'],
                                      '32VKN',dem_source='ewoc',data_source='aws')

    def test_cli_cop_dem_32VKN(self):
        """Test with cop dem retrieve from aws bucket.

        The 32VKN tile is outside the srtm coverage
        """
        with self.assertRaises(S1DEMProcessorError):
            generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20210310T055600_20210310T055625_036932_045843_9A14',
                                       'S1A_IW_GRDH_1SDV_20210310T055625_20210310T055650_036932_045843_0613'],
                                      '32VKN',dem_source='aws',data_source='aws', clean=False)


    def test_cli_cop_dem_31TCJ(self):
        """Test with cop dem retrieve from aws bucket

        The 31TCJ tile is inside the srtm coverage (used for comparison)
        """
        (a,b)=generate_s1_ard_from_pids(['S1A_IW_GRDH_1SDV_20210708T060040_20210708T060105_038682_04908E_3178',
                                         'S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979'],
                                        '31TCJ',dem_source='aws',data_source='aws', clean=False)

if __name__ == "__main__":
    unittest.main()
