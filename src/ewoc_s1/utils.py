import configparser
import json
import logging
from pathlib import Path
from typing import Dict, List

from ewoc_s1.s1_prd_id import S1PrdIdInfo

logger = logging.getLogger(__name__)

def to_s1tiling_configfile(out_dirpath, s1_input_dirpath, dem_dirpath, working_dirpath, s2_tile_id):
# TODO manage processing parameters


    config = configparser.ConfigParser()
    config['Paths'] = {'output': str(out_dirpath),
                       's1_images': str(s1_input_dirpath),
                       'srtm': str(dem_dirpath),
                       'tmp': str(working_dirpath)}

    config['Processing'] = {'mode' : 'debug' + ' ' + 'logging',
                            'calibration': 'sigma',
                            'remove_thermal_noise': True,
                            'output_spatial_resolution' : 20.,
                            'orthorectification_gridspacing' : 80,
                            'orthorectification_interpolation_method' : 'linear',
                            'tiles': s2_tile_id,
                            'tile_to_product_overlap_ratio' : 0.5,
                            'nb_parallel_processes' : 2,
                            'ram_per_process' : 4096,
                            'nb_otb_threads': 2,
                            }

    config['DataSource'] = {'download' : False,
                            'roi_by_tiles' : 'ALL',
                            'first_date' : '2016-06-01',
                            'last_date' : '2025-07-31',
                            'polarisation' : 'VV-VH'}
    config['Mask'] = {'generate_border_mask' : False}

    config_filepath = working_dirpath / 'S1Processor.cfg'
    with open(config_filepath, 'w') as configfile:
        config.write(configfile)

    return config_filepath

class EwocWorkPlanReader():
    PROD_TYPE = {"S2":"S2_PROC","L8":"L8_PROC","S1":"SAR_PROC"}    

    def __init__(self, workplan_filepath: Path) -> None:
        with open(workplan_filepath) as f:
            self._wp = json.load(f)
        
        self._tile_ids = list()
        for tile in self._wp:
            self._tile_ids.append(tile)

    @property
    def tile_ids(self)-> List[str]:
        return self._tile_ids

    def get_nb_s1_prd(self, tile_id:str)->int:
        if tile_id in self._tile_ids:
            return len(self._wp[tile_id]['SAR_PROC']['INPUTS'])
        else:
            return 0

    def get_s1_prd_ids(self, tile_id:str)-> List[str]:
        if tile_id in self._tile_ids:
            return self._wp[tile_id]['SAR_PROC']['INPUTS']
        else:
            return None
    
    def get_s1_prd_ids_by_date(self, tile_id: str)-> Dict:
        prd_ids = self.get_s1_prd_ids(tile_id)
        out = dict()
        for prd_id in prd_ids:
            out[str(S1PrdIdInfo(prd_id[0]).start_time.date())] = prd_id

        return out
