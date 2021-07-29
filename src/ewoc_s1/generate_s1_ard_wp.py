
import argparse
import logging
from pathlib import Path
import shutil
import sys

from dataship.dag.utils import get_product_by_id
from dataship.dag.s3man import recursive_upload_dir_to_s3, get_s3_client

from s1tiling.S1Processor import main as s1_process

from ewoc_s1 import __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo
from ewoc_s1.ewoc_s1_ard import to_ewoc_s1_ard
from ewoc_s1.utils import EwocWorkPlanReader, to_s1tiling_configfile

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def generate_s1_ard_wp(work_plan_filepath, out_dirpath_root, conf_filepath, dem_dirpath=None, working_dirpath_root=None):
    working_dirpath = working_dirpath_root / 'ewoc_s1_wp'
    working_dirpath.mkdir(exist_ok=True)

    out_dirpath = out_dirpath_root / 'ewoc_s1_ard'
    out_dirpath.mkdir(exist_ok=True)

    logger.info('Work plan: %s', work_plan_filepath)
    logger.info('out_dirpath: %s', out_dirpath)
    logger.info('dem_dirpath: %s', dem_dirpath)
    logger.info('working_dirpath: %s', working_dirpath)

    # Extract from workplan each S1 product id for each tile to process
    wp_reader = EwocWorkPlanReader(work_plan_filepath)
    logger.info(wp_reader.tile_ids)
    
    logger.info('%s tiles will be process!', len(wp_reader.tile_ids))
    for s2_tile_id in wp_reader.tile_ids:
        
        logger.info(wp_reader.get_s1_prd_ids(s2_tile_id))
        # TODO manage S1 product to retrieve date by date

        s1_input_dirpath_tile = working_dirpath / 'input' / s2_tile_id
        s1_input_dirpath_tile.mkdir(exist_ok=True, parents=True)
        
        #for s1_prd_id in wp_reader.get_s1_prd_ids(s2_tile_id):
        for date_key, s1_prd_ids in wp_reader.get_s1_prd_ids_by_date(s2_tile_id).items():
            logger.info('%s will be process for %s!', s1_prd_ids, date_key)
            s1_input_dirpath_tile_date = s1_input_dirpath_tile / date_key
            s1_input_dirpath_tile_date.mkdir(exist_ok=True)
            out_dirpath_date = out_dirpath / date_key
            for s1_prd_id in s1_prd_ids: 
                if S1PrdIdInfo.is_valid(s1_prd_id):
                    if len(s1_prd_id.split('.'))==1:
                        s1_prd_id = s1_prd_id + '.SAFE'
                    s1_prd_safe_dirpath = s1_input_dirpath_tile_date / s1_prd_id
                    s1_prd_wsafe_dirpath =  s1_input_dirpath_tile_date / s1_prd_safe_dirpath.stem
                    if not s1_prd_wsafe_dirpath.exists():
                        get_product_by_id(s1_prd_id, s1_input_dirpath_tile_date, 'creodias', config_file=conf_filepath)
                        s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
                    else:
                        logger.info('S1 prd %s is already available on disk', s1_prd_id)
                else:
                    logger.warning('S1 prd id %s is not valid!', s1_prd_id)

            s1_process.callback(20, False, False, False, False, False,
                                to_s1tiling_configfile(out_dirpath_date,
                                                s1_input_dirpath_tile_date,
                                                dem_dirpath,
                                                working_dirpath,
                                                s2_tile_id))

            to_ewoc_s1_ard( out_dirpath_date / s2_tile_id, out_dirpath, 
                            S1PrdIdInfo(s1_prd_ids[0]), s2_tile_id, 
                            rename_only=False)

            # TODO Push to bucket if requested
            logger.info('Push %s to bucket', out_dirpath_date)
            recursive_upload_dir_to_s3(get_s3_client(), out_dirpath, 'WORLDCEREAL_PREPROC/test_upload', bucketname="world_cereal")

            logger.info('Remove %s', out_dirpath_date)
            shutil.rmtree(out_dirpath_date)


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Generate EWoC S1 ARD from a EWOC workplan")
    parser.add_argument(
        "--version",
        action="version",
        version="ewoc_s1 {ver}".format(ver=__version__),
    )
    parser.add_argument(dest="work_plan", help="EWoC Working plan", type=Path)
    parser.add_argument(dest="out_dirpath", help="Output Dirpath", type=Path)
    parser.add_argument("--dem_dirpath", dest="dem_dirpath", help="DEM dirpath", type=Path)
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path)
    parser.add_argument("-c", dest="conf_filepath", help="eodag conf filepath", type=Path)
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    """Wrapper allowing :func:`generate_s1_ard_wp` to be called with string arguments in a CLI fashion

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    logger.debug("Starting Generate S1 ARD for the workplan ...", args.work_plan)
    generate_s1_ard_wp(args.work_plan, args.out_dirpath, args.conf_filepath,
                       args.dem_dirpath, args.working_dirpath)
    logger.info("Generation of the EWoC workplan %s for S1 part is ended!", args.work_plan)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m ewoc_s1.generate_ard 42
    #
    run()
