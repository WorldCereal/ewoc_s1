from datetime import datetime
import pytest

from ewoc_s1.s1_prd_id import S1PrdIdInfo

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"


def test_s1_prd_info():
    """API Tests"""
    test_s1_prd_id = 'S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979.SAFE'
    s1_prd_info = S1PrdIdInfo(test_s1_prd_id)
    assert s1_prd_info.mission_id == 'S1A'
    assert s1_prd_info.beam_mode == 'IW'
    assert s1_prd_info.product_type == 'GRD'
    assert s1_prd_info.resolution_class == 'H'
    assert s1_prd_info.processing_level == '1'
    assert s1_prd_info.product_class == 'S'
    assert s1_prd_info.polarisation == 'DV'
    assert s1_prd_info.start_time == datetime()
    assert s1_prd_info.stop_time == datetime()
    assert s1_prd_info.absolute_orbit_number == '038682'
    assert s1_prd_info.mission_datatake_id == '04908E'
    assert s1_prd_info.product_unique_id() == '8979'

    # with pytest.raises(ValueError):
    #     test

