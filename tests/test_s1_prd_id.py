from datetime import datetime
import unittest


from ewoc_s1.s1_prd_id import S1PrdIdInfo

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

class Test_S1PrdIdInfo(unittest.TestCase):
    def test_s1_prd_info(self):
        """API Tests"""
        test_s1_prd_id = 'S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979.SAFE'
        s1_prd_info = S1PrdIdInfo(test_s1_prd_id)
        self.assertEqual(s1_prd_info.mission_id, 'S1A')
        self.assertEqual(s1_prd_info.beam_mode , 'IW')
        self.assertEqual(s1_prd_info.product_type , 'GRD')
        self.assertEqual(s1_prd_info.resolution_class , 'H')
        self.assertEqual(s1_prd_info.processing_level , '1')
        self.assertEqual(s1_prd_info.product_class , 'S')
        self.assertEqual(s1_prd_info.polarisation , 'DV')
        self.assertEqual(s1_prd_info.start_time ,
            datetime.strptime('20210708T060105','%Y%m%dT%H%M%S'))
        self.assertEqual(s1_prd_info.stop_time ,
            datetime.strptime('20210708T060130','%Y%m%dT%H%M%S'))
        self.assertEqual(s1_prd_info.absolute_orbit_number , '038682')
        self.assertEqual(s1_prd_info.mission_datatake_id , '04908E')
        self.assertEqual(s1_prd_info.product_unique_id , '8979')

if __name__ == "__main__":
    unittest.main()
