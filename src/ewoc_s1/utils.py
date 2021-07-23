import configparser
import json

from ewoc_s1.s1_prd_id import S1PrdIdInfo


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
                            'ram_per_process' : 8096,
                            'nb_otb_threads': 4,
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

    def __init__(self, workplan_filepath) -> None:
        with open(workplan_filepath) as f:
            self._wp = json.load(f)
        
        self._tile_ids = list()
        for tile in self._wp:
            self._tile_ids.append(tile)

        # split name to retrieve start_date and end_date

    @property
    def tile_ids(self):
        return self._tile_ids

    def get_s1_prd_ids(self, tile_id):
        if tile_id in self._tile_ids:
            return self._wp[tile_id]['SAR_PROC']['INPUTS']
        else:
            return None
    
    def get_s1_prd_ids_by_date(self, tile_id):
        prd_ids = self.get_s1_prd_ids(tile_id)
        out = dict()
        for prd_id in prd_ids:
            date_key = str(S1PrdIdInfo(prd_id).start_time.date())
            if date_key in out.keys():
                out[date_key] = out.get(date_key) + [prd_id]
            else:
                out[date_key] = [prd_id]
        return out