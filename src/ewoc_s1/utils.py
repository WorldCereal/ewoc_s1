import configparser
import json
import logging
from pathlib import Path
from typing import Dict, List

from psutil import cpu_count, virtual_memory

from ewoc_s1.s1_prd_id import S1PrdIdInfo

logger = logging.getLogger(__name__)

def to_s1tiling_configfile(out_dirpath: Path, s1_input_dirpath: Path, dem_dirpath: Path, working_dirpath: Path, s2_tile_id: str, 
                           cluster_config,
                           calibration_method :str= 'sigma', output_spatial_resolution: int=20, remove_thermal_noise: bool=True, 
                           ortho_interpol_method:str='linear', generate_mask: bool=False, log_level:int = logging.INFO):

    optimal_ram, optimal_nb_process, optimal_nb_otb_threads = cluster_config.compute_optimal_cluster_config()

    config = configparser.ConfigParser()
    config['Paths'] = {'output': str(out_dirpath),
                       's1_images': str(s1_input_dirpath),
                       'srtm': str(dem_dirpath),
                       'tmp': str(working_dirpath)}

    if log_level < logging.DEBUG:
        s1_process_log_mode = 'debug' + ' ' + 'logging'
    else:
        s1_process_log_mode = 'Normal'

    config['Processing'] = {'mode' : s1_process_log_mode,
                            'calibration': calibration_method,
                            'remove_thermal_noise': remove_thermal_noise,
                            'output_spatial_resolution' : output_spatial_resolution,
                            'orthorectification_gridspacing' : 4*output_spatial_resolution,
                            'orthorectification_interpolation_method' : ortho_interpol_method,
                            'tiles': s2_tile_id,
                            'tile_to_product_overlap_ratio' : 0.5,
                            'nb_parallel_processes' : optimal_nb_process,
                            'ram_per_process' : optimal_ram,
                            'nb_otb_threads': optimal_nb_otb_threads,
                            }

    config['DataSource'] = {'download' : False,
                            'roi_by_tiles' : 'ALL',
                            'first_date' : '2016-06-01',
                            'last_date' : '2025-07-31',
                            'polarisation' : 'VV-VH'}
    config['Mask'] = {'generate_border_mask' : generate_mask}

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

class ClusterConfig():
    def __init__(self, nb_products:int) -> None:

        if nb_products < 1:
            raise ValueError
        self._nb_products= nb_products
        self._physical_core = cpu_count(logical=False)
        self._total_core = cpu_count(logical=True)
        self._total_ram = virtual_memory().total


    @property
    def physical_core(self):
        return self._physical_core

    @property
    def total_core(self):
        return self._total_core

    @property
    def total_ram(self):
        return self._total_ram

    def compute_optimal_cluster_config(self, ram_scale_factor=0.95):

        optimal_nb_process = 2 * self._nb_products
        if optimal_nb_process > self._physical_core:
            optimal_nb_process = self.physical_core

        optimal_ram = int(ram_scale_factor* (self._total_ram / optimal_nb_process))

        optimal_nb_otb_threads = int(self._total_core / optimal_nb_process)

        mb_factor = 1024*1024
        logger.info('Optimal RAM for %s product(s) = %s / %s',  self._nb_products,
                                                                int(optimal_ram/mb_factor),
                                                                int(self._total_ram/mb_factor))
        logger.info('Optimal nb process for %s product(s) = %s', self._nb_products, optimal_nb_process)
        logger.info('Optimal nb otb threads for %s product(s) = %s / %s / %s', self._nb_products,
                                                                              optimal_nb_otb_threads,
                                                                              self._physical_core,
                                                                              self._total_core)

        return int(optimal_ram/mb_factor), optimal_nb_process, optimal_nb_otb_threads
