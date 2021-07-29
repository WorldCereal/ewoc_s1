
import argparse
import logging
from pathlib import Path
import sys
import shutil

from dataship.dag.s3man import get_s3_client, recursive_upload_dir_to_s3
from dataship.dag.utils import get_product_by_id
from s1tiling.S1Processor import clean_logs, main as s1_process

from ewoc_s1 import __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo
from ewoc_s1.ewoc_s1_ard import to_ewoc_s1_ard
from ewoc_s1.utils import to_s1tiling_configfile

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)



def generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath, conf_filepath,
                        dem_dirpath=None, working_dirpath=None, clean_output=False, upload_outputs=True, clean_input_dir=False):

    logger.info('Product ids: %s', s1_prd_ids)
    logger.info('s2_tile_id: %s', s2_tile_id)
    logger.info('out_dirpath: %s', out_dirpath)
    logger.info('dem_dirpath: %s', dem_dirpath)
    logger.info('working_dirpath: %s', working_dirpath)


def generate_s1_ard_id(s1_prd_ids, s2_tile_id, out_dirpath_root, conf_filepath,  
                        dem_dirpath=None, working_dirpath_root=None, clean_output=False, upload_outputs=True, clean_input_dir=False):
    working_dirpath = working_dirpath_root / 'ewoc_s1'
    working_dirpath.mkdir(exist_ok=True)

    out_dirpath = out_dirpath_root / 'ewoc_s1_ard'
    out_dirpath.mkdir(exist_ok=True)

    logger.info('Product ids: %s', s1_prd_ids)
    logger.info('s2_tile_id: %s', s2_tile_id)
    logger.info('out_dirpath: %s', out_dirpath)
    logger.info('dem_dirpath: %s', dem_dirpath)
    logger.info('working_dirpath: %s', working_dirpath)

    s1_input_dir = working_dirpath /'input'
    s1_input_dir.mkdir(exist_ok=True)

    for s1_prd_id in s1_prd_ids:
        if S1PrdIdInfo.is_valid(s1_prd_id):
            s1_prd_safe_dirpath = s1_input_dir / s1_prd_id
            s1_prd_wsafe_dirpath =  s1_input_dir / s1_prd_safe_dirpath.stem
            if not s1_prd_wsafe_dirpath.exists():
                try:
                    get_product_by_id(s1_prd_id, s1_input_dir, 'creodias', config_file=conf_filepath)
                except:
                    logger.error('No product have been retrieve for %s', s1_prd_id)
                    continue
                s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
            else:
                logger.info('S1 prd %s is already available on disk', s1_prd_id)
        else:
            logger.warning('S1 prd id %s is not valid!', s1_prd_id)

    
    output_s1process_dirpath = out_dirpath / 's1process'
    output_s1process_dirpath.mkdir(exist_ok=True)
    s1_process.callback(20, False, False, False, False, False, 
                        to_s1tiling_configfile(out_dirpath, 
                                               s1_input_dir, 
                                               dem_dirpath, 
                                               working_dirpath, 
                                               s2_tile_id))

    # If sucess of s1process remove the input dir
    if clean_input_dir:
        shutil.rmtree(s1_input_dir)
        s1_input_dir.rmdir()

    out_dirpath_tile_id = out_dirpath / s2_tile_id
    clean_s1_process_output = True
    to_ewoc_s1_ard( out_dirpath_tile_id, out_dirpath, 
                    S1PrdIdInfo(s1_prd_ids[0]), s2_tile_id, 
                    rename_only=False, clean_input_file=clean_s1_process_output)
    
    # if sucess of format output, remove the s1 process output dir
    if clean_s1_process_output:
        shutil.rmtree(out_dirpath_tile_id)
        out_dirpath_tile_id.rmdir()

    logger.info('Push %s to bucket', out_dirpath)
    recursive_upload_dir_to_s3( get_s3_client(), 
                                str(out_dirpath) + '/', 
                                'WORLDCEREAL_PREPROC/test_upload/', 
                                bucketname="world-cereal")

    # if sucess remove the previous output
    if clean_output:
        logger.info('Remove %s', out_dirpath)
        shutil.rmtree(out_dirpath)


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
    parser = argparse.ArgumentParser(description="Generate EWoC S1 ARD from S1 Product IDs over S2 Tile Id")
    parser.add_argument(
        "--version",
        action="version",
        version="ewoc_s1 {ver}".format(ver=__version__),
    )
    parser.add_argument(dest="s2_tile_id", help="Sentinel-2 Tile ID", type=str)
    parser.add_argument(dest="out_dirpath", help="Output Dirpath", type=Path)
    parser.add_argument(dest="s1_prd_ids", help="Sentinel-1 Product ids", nargs='*')
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
    """Wrapper allowing :func:`generate_s1_ard` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    logger.debug("Starting Generate S1 ARD for %s over %s MGRS Tile ...", args.s1_prd_ids, args.s2_tile_id)
    generate_s1_ard_id(args.s1_prd_ids, args.s2_tile_id, args.out_dirpath, args.conf_filepath,
                            args.dem_dirpath, args.working_dirpath)
    logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!", args.s1_prd_ids, args.s2_tile_id)


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
