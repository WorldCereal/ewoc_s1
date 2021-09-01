import logging
import os
from pathlib import Path
import shutil
from typing import List

from dataship.dag.s3man import get_s3_client, recursive_upload_dir_to_s3
from dataship.dag.s1_dag import get_s1_product
from s1tiling.S1Processor import main as s1_process


from ewoc_s1 import __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo
from ewoc_s1.ewoc_s1_ard import to_ewoc_s1_ard
from ewoc_s1.utils import ClusterConfig, to_s1tiling_configfile

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def generate_s1_ard(s1_prd_ids: List[str], s2_tile_id: str, out_dirpath_root: Path,
                    dem_dirpath: Path, working_dirpath: Path,
                    clean: bool=True, upload_outputs: bool=True, data_source:str='creodias_eodata'):

    """ Generate S1 ARD from the products identified by their product id for the S2 tile id
    """

    out_dirpath = out_dirpath_root / 'ewoc_s1_ard'
    out_dirpath.mkdir(exist_ok=True)

    logger.info('Product ids: %s', s1_prd_ids)
    logger.info('s2_tile_id: %s', s2_tile_id)
    logger.info('out_dirpath: %s', out_dirpath)
    logger.info('dem_dirpath: %s', dem_dirpath)
    logger.info('working_dirpath: %s', working_dirpath)

    s1_input_dir = working_dirpath / 'input' / s2_tile_id
    s1_input_dir.mkdir(exist_ok=True, parents=True)

    wd_s1process_dirpath_root = working_dirpath / 's1process'
    wd_s1process_dirpath_root.mkdir(exist_ok=True)
    output_s1process_dirpath = wd_s1process_dirpath_root / s2_tile_id
    
    wd_s1process_noized_dirpath_root = working_dirpath / 's1process_noized'
    wd_s1process_noized_dirpath_root.mkdir(exist_ok=True)
    output_s1process_noized_dirpath = wd_s1process_noized_dirpath_root / s2_tile_id

    for s1_prd_id in s1_prd_ids:
        if S1PrdIdInfo.is_valid(s1_prd_id):
            if len(s1_prd_id.split('.'))==1:
                s1_prd_id = s1_prd_id + '.SAFE'
            s1_prd_safe_dirpath = s1_input_dir / s1_prd_id
            s1_prd_wsafe_dirpath =  s1_input_dir / s1_prd_safe_dirpath.stem
            if not s1_prd_wsafe_dirpath.exists():
                try:
                    get_s1_product(s1_prd_id, s1_input_dir, source=data_source)
                except:
                    logger.error('No product download for %s', s1_prd_id)
                    continue
                if data_source == 'creodias_finder':
                    s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
                else:
                    s1_prd_wsafe_dirpath.mkdir()
                    s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath/s1_prd_safe_dirpath.name)
            else:
                logger.info('S1 prd %s is already available on disk', s1_prd_id)
        else:
            logger.warning('S1 prd id %s is not valid!', s1_prd_id)

    if not any(s1_input_dir.iterdir()):
        logger.error('No S1 products downloaded!')
        return

    try:
        s1_process.callback(20, False, False, False, False, False, 
                            to_s1tiling_configfile(wd_s1process_dirpath_root, 
                                                   s1_input_dir, 
                                                   dem_dirpath, 
                                                   wd_s1process_dirpath_root, 
                                                   s2_tile_id, ClusterConfig(len(s1_prd_ids))))
    except:
        logger.error('S1 process denoized failed!')
        return

    try:
        s1_process.callback(20, False, False, False, False, False, 
                            to_s1tiling_configfile(wd_s1process_noized_dirpath_root, 
                                                   s1_input_dir, 
                                                   dem_dirpath, 
                                                   wd_s1process_noized_dirpath_root, 
                                                   s2_tile_id, 
                                                   ClusterConfig(len(s1_prd_ids)),
                                                   remove_thermal_noise=False))
    except:
        logger.error('S1 process noized failed!')
        return
    
    # If sucess of s1process remove the input dir
    if clean:
        shutil.rmtree(s1_input_dir)

    try:
        to_ewoc_s1_ard( output_s1process_dirpath, out_dirpath, 
                        S1PrdIdInfo(s1_prd_ids[0]), s2_tile_id, 
                        rename_only=False, clean_input_file=clean)
    except:
        logger.error('Format to ewoc product failed!')
        return
        
    # if sucess of format output, remove the s1 process wd dir
    if clean:
        shutil.rmtree(wd_s1process_dirpath_root)

    if upload_outputs:
        logger.info('Push %s to bucket', out_dirpath)
        try:
            recursive_upload_dir_to_s3( get_s3_client(), 
                                        str(out_dirpath) + '/', 
                                        os.getenv('DEST_PREFIX', default = 'WORLDCEREAL_PREPROC/test_upload/'), 
                                        bucketname=os.getenv('BUCKET', default='world-cereal'))
        except:
            logger.error('Push to ewoc bucket failed!')
            return

        # if sucess remove from disk the data pushed to the bucket
        if clean:
            logger.info('Remove %s', out_dirpath)
            shutil.rmtree(out_dirpath)