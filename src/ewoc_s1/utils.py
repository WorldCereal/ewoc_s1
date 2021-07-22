import configparser

# TODO manage processing parameters

def to_s1tiling_configfile(out_dirpath, s1_input_dirpath, dem_dirpath, working_dirpath, s2_tile_id):
    
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