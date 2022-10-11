import logging
from pathlib import Path
import shutil
from typing import List, Tuple, final

from ewoc_dag.bucket.ewoc import EWOCARDBucket
from ewoc_dag.s1_dag import get_s1_product, S1DagError
from ewoc_dag.safe_format import S1SafeConversionError
from s1tiling.S1Processor import s1_process

from ewoc_s1 import EWOC_S1_INPUT_DOWNLOAD_ERROR, EWOC_S1_PROCESSOR_ERROR, EWOC_S1_ARD_FORMAT_ERROR, __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo
from ewoc_s1.ewoc_s1_ard import to_ewoc_s1_ard
from ewoc_s1.utils import ClusterConfig, to_s1tiling_configfile

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

class S1ARDProcessorBaseError(Exception):
    """ Base Error"""
    def __init__(self, exit_code, s1_prd_ids):
        self._message = "Error during S1 ARD generation:"
        self.exit_code = exit_code
        self._s1_prd_ids = s1_prd_ids
        super().__init__(self._message)

class S1InputProcessorError(S1ARDProcessorBaseError):
    """Exception raised for errors in the S1 ARD generation at input download step."""

    def __init__(self, prd_ids, s1_data_source):
        super().__init__(EWOC_S1_INPUT_DOWNLOAD_ERROR, prd_ids)
        self._s1_data_source = s1_data_source

    def __str__(self):
        return f"{self._message} {self._s1_prd_ids} not download from {self._s1_data_source}!"

class S1ProcessorError(S1ARDProcessorBaseError):
    """Exception raised for errors in the S1 ARD generation at S1 tiling step."""

    def __init__(self, prd_ids, s2_tile_id, with_thermal_noise_removal=True):
        super().__init__(EWOC_S1_PROCESSOR_ERROR, prd_ids)
        self._s2_tile_id = s2_tile_id
        self._with_thermal_noise_removal = with_thermal_noise_removal

    def __str__(self):
        if self._with_thermal_noise_removal:
            return f"{self._message} {self._s1_prd_ids} not process to {self._s2_tile_id} with S1 Tiling (with thermal noise removal)!"
        else:
            return f"{self._message} {self._s1_prd_ids} not process to {self._s2_tile_id} with S1 Tiling (without thermal noise removal)!"

class S1ARDFormatError(S1ARDProcessorBaseError):
    """Exception raised for errors in the S1 ARD generation at ARD format step."""

    def __init__(self, prd_ids):
        super().__init__(EWOC_S1_ARD_FORMAT_ERROR, prd_ids)

    def __str__(self):
        return f"{self._message} Failed to convert EWoC ARD format!"


def generate_s1_ard(s1_prd_ids: List[str], s2_tile_id: str, out_dirpath_root: Path,
                    dem_dirpath: Path, working_dirpath: Path,
                    clean: bool=True, upload_outputs: bool=True, data_source:str='creodias',
                    production_id:str=None)-> Tuple[int, str]:

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
                    get_s1_product(s1_prd_id, out_root_dirpath=s1_input_dir, source=data_source, safe_format=True)
                except S1DagError as exc:
                    logger.warning(exc)
                    logger.warning('No product download for %s from %s', s1_prd_id, data_source)
                    # Clean empty dir to avoid confusion from s1tiling
                    if s1_prd_safe_dirpath.exists():
                        s1_prd_safe_dirpath.rmdir()
                    if s1_prd_wsafe_dirpath.exists():
                        s1_prd_wsafe_dirpath.rmdir()

                else:
                    if data_source == 'eodag':
                        s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
                    else:
                        s1_prd_wsafe_dirpath.mkdir()
                        s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath/s1_prd_safe_dirpath.name)
            else:
                logger.info('S1 prd %s is already available on disk', s1_prd_id)
        else:
            logger.warning('S1 prd id %s is not valid!', s1_prd_id)
            # TODO remove the corresponding id from the list ?

    if not any(s1_input_dir.iterdir()):
        s1_input_dir.rmdir()
        raise S1InputProcessorError(s1_prd_ids, data_source)

    try:
        s1_process(str(to_s1tiling_configfile(wd_s1process_dirpath_root,
                                            s1_input_dir,
                                            dem_dirpath,
                                            wd_s1process_dirpath_root,
                                            s2_tile_id, ClusterConfig(len(s1_prd_ids)))))
        logger.info('S1 process with thermal noise removal done!')
    except:
        if clean:
            shutil.rmtree(s1_input_dir)
        raise S1ProcessorError(s1_prd_ids, s2_tile_id)
   
    try:
        s1_process(str(to_s1tiling_configfile(wd_s1process_noized_dirpath_root,
                                            s1_input_dir,
                                            dem_dirpath,
                                            wd_s1process_noized_dirpath_root,
                                            s2_tile_id,
                                            ClusterConfig(len(s1_prd_ids)),
                                            remove_thermal_noise=False)))
        logger.info('S1 process without thermal noise removal done!')
    except:
        raise S1ProcessorError(s1_prd_ids, s2_tile_id, with_thermal_noise_removal=False)
    finally:
        if clean:
            shutil.rmtree(s1_input_dir)

    try:
        to_ewoc_s1_ard( output_s1process_dirpath, out_dirpath,
                        S1PrdIdInfo(s1_prd_ids[0]), s2_tile_id,
                        rename_only=False, clean_input_file=clean)
        logger.info('Successful convertion to EWoC ARD format!')
    except:
        raise S1ARDFormatError(s1_prd_ids)
    finally:
        if clean:
            shutil.rmtree(wd_s1process_dirpath_root)
            shutil.rmtree(s1_input_dir)

    if upload_outputs:
        try:
            logger.info('Try to push %s to EWoC ARD bucket', out_dirpath)
            nb_s1_ard_file, __unused, s1_ard_s3path = \
                EWOCARDBucket().upload_ard_prd(out_dirpath, production_id)
            logger.info("Succeed to upload %s S1 ARD files to %s",
                nb_s1_ard_file, s1_ard_s3path)
        except:
            logger.error('Push to EWoC ARD bucket failed!')
            raise RuntimeError("Generate S1 ARD failed during the upload of ARD data to bucket")

        # if sucess remove from disk the data pushed to the bucket
        if clean:
            shutil.rmtree(out_dirpath)

        return nb_s1_ard_file, s1_ard_s3path
    return 2, ''
