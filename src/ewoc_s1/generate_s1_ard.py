
import logging
import shutil

from dataship.dag.s3man import get_s3_client, recursive_upload_dir_to_s3
from dataship.dag.utils import get_product_by_id
from s1tiling.S1Processor import main as s1_process

from ewoc_s1 import __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo
from ewoc_s1.ewoc_s1_ard import to_ewoc_s1_ard
from ewoc_s1.utils import to_s1tiling_configfile

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath_root,
                    dem_dirpath, working_dirpath,
                    clean=True, upload_outputs=True):

    """ Generate S1 ARD from the products identified by their product id for the S2 tile id
    """

    # TODO: manage strm1s tile needed for the s2 tile and download them if needed

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

    for s1_prd_id in s1_prd_ids:
        if S1PrdIdInfo.is_valid(s1_prd_id):
            s1_prd_safe_dirpath = s1_input_dir / s1_prd_id
            s1_prd_wsafe_dirpath =  s1_input_dir / s1_prd_safe_dirpath.stem
            if not s1_prd_wsafe_dirpath.exists():
                try:
                    get_product_by_id(s1_prd_id, s1_input_dir, 'creodias')
                except:
                    logger.error('No product download for %s', s1_prd_id)
                    continue
                s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
            else:
                logger.info('S1 prd %s is already available on disk', s1_prd_id)
        else:
            logger.warning('S1 prd id %s is not valid!', s1_prd_id)

    # TODO check if the input dir is empty (no sucessful download)
    if not any(s1_input_dir.iterdir()):
        logger.error('No S1 products downloaded!')
        return

    try:
        s1_process.callback(20, False, False, False, False, False, 
                            to_s1tiling_configfile(wd_s1process_dirpath_root, 
                                                   s1_input_dir, 
                                                   dem_dirpath, 
                                                   wd_s1process_dirpath_root, 
                                                   s2_tile_id))
    except:
        logger.error('S1 process failed!')
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
                                        'WORLDCEREAL_PREPROC/test_upload/', 
                                        bucketname="world-cereal")
        except:
            logger.error('Push to ewoc bucket failed!')
            return

        # if sucess remove from disk the data pushed to the bucket
        if clean:
            logger.info('Remove %s', out_dirpath)
            shutil.rmtree(out_dirpath)