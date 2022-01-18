
import argparse
from datetime import datetime
import logging
from pathlib import Path
import sys
import shutil
from tempfile import gettempdir
from typing import List

from ewoc_dag.srtm_dag import get_srtm_from_s2_tile_id

from ewoc_s1 import __version__
from ewoc_s1.generate_s1_ard import generate_s1_ard
from ewoc_s1.utils import EwocWorkPlanReader

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def _get_default_prod_id()->str:
    str_now=datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"0000_000_{str_now}"

def generate_s1_ard_wp(work_plan_filepath, out_dirpath_root,
                       dem_dirpath=None, working_dirpath_root=Path(gettempdir()),
                       clean=True, upload_outputs=True,
                       data_source='creodias_eodata', dem_source='creodias'):

    working_dirpath = working_dirpath_root / 'ewoc_s1_wp'
    working_dirpath.mkdir(exist_ok=True)

    logger.info('Work plan: %s', work_plan_filepath)

    wp_reader = EwocWorkPlanReader(work_plan_filepath)
    logger.info('%s tiles will be process: %s!',
                len(wp_reader.tile_ids), wp_reader.tile_ids)

    for s2_tile_id in wp_reader.tile_ids:
        logger.info('Generate %s ARD for the S2 tile: %s!', wp_reader.get_nb_s1_prd(s2_tile_id),
                                                            s2_tile_id)

        wd_dirpath_tile = working_dirpath / s2_tile_id
        wd_dirpath_tile.mkdir(exist_ok=True, parents=True)

        if dem_dirpath is None:
            dem_dirpath = wd_dirpath_tile / 'dem'
            dem_dirpath.mkdir(exist_ok=True, parents=True)
            try:
                get_srtm_from_s2_tile_id(s2_tile_id, dem_dirpath, source=dem_source)
            except:
                logger.critical('No elevation available!')
                return

        for date_key, s1_prd_ids in wp_reader.get_s1_prd_ids_by_date(s2_tile_id).items():
            logger.info('%s will be process for %s!', s1_prd_ids, date_key)

            wd_dirpath_tile_date = wd_dirpath_tile / date_key
            wd_dirpath_tile_date.mkdir(exist_ok=True)

            generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath_root,
                            dem_dirpath, wd_dirpath_tile_date,
                            clean=clean, upload_outputs=upload_outputs,
                            data_source=data_source)

            if clean:
                shutil.rmtree(wd_dirpath_tile_date)
        if clean:
            shutil.rmtree(wd_dirpath_tile)
    if clean:
        shutil.rmtree(working_dirpath)


def generate_s1_ard_from_pids(s1_prd_ids, s2_tile_id, out_dirpath_root,
                        dem_dirpath=None, working_dirpath_root=Path(gettempdir()),
                        clean=False, upload_outputs=False,
                        data_source='creodias_eodata', dem_source='creodias'):


    working_dirpath = working_dirpath_root / 'ewoc_s1_pid'
    working_dirpath.mkdir(exist_ok=True)

    if dem_dirpath is None:
        dem_dirpath = working_dirpath / 'dem' / s2_tile_id
        dem_dirpath.mkdir(exist_ok=True, parents=True)
        try:
            get_srtm_from_s2_tile_id(s2_tile_id,
                out_dirpath= dem_dirpath,
                source=dem_source)
        except:
            logger.critical('No elevation available!')
            return

    s1_ard_keys = generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath_root,
                    dem_dirpath, working_dirpath,
                    clean=clean, upload_outputs=upload_outputs,
                    data_source=data_source)

    if clean:
        shutil.rmtree(working_dirpath)

    return s1_ard_keys

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args:List[str]):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate EWoC S1 ARD")
    parser.add_argument(
        "--version",
        action="version",
        version=f"ewoc_s1 {__version__}",
    )
    parser.add_argument("-o", "--out_dir",
        dest="out_dirpath",
        help="Output Dirpath",
        type=Path,
        default=Path(gettempdir()))
    parser.add_argument("--dem_dirpath", dest="dem_dirpath", help="DEM dirpath", type=Path)
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path,
        default=Path(gettempdir()))

    parser.add_argument("--no-clean",
        action='store_true',
        help= 'Avoid to clean all dirs and files')
    parser.add_argument("--no-upload",
        action='store_true',
        help= 'Skip the upload of ard files to s3 bucket')

    parser.add_argument("--prod-id",
        dest="prod_id",
        help="Production ID that will be used to upload to s3 bucket, by default it is computed internally")


    parser.add_argument("--data-source", dest="data_source", help= 'Source of the S1 input data',
                        type=str,
                        default='creodias_eodata')
    parser.add_argument("--dem-source", dest="dem_source", help= 'Source of the DEM data',
                        type=str,
                        default='creodias_eodata')
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

    subparsers = parser.add_subparsers(dest='subparser_name')

    parser_prd_ids = subparsers.add_parser('prd_ids',
        help='Generate EWoC S1 ARD from S1 GRD product IDs')

    parser_prd_ids.add_argument(dest="s2_tile_id", help="Sentinel-2 Tile ID", type=str)
    parser_prd_ids.add_argument(dest="s1_prd_ids", help="Sentinel-1 Product ids", nargs='*')

    parser_wp = subparsers.add_parser('wp', help='Generate EWoC L8 ARD from EWoC workplan')
    parser_wp.add_argument(dest="wp",
        help="EWoC workplan in json format",
        type=Path)

    args = parser.parse_args(args)

    if args.subparser_name is None:
        parser.print_help()

    return args


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args:List[str]):
    """Wrapper allowing :func:`generate_s1_ard` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    logger.debug(args)

    if args.subparser_name == "prd_ids":

        logger.debug("Starting Generate S1 ARD for %s over %s MGRS Tile ...", args.s1_prd_ids, args.s2_tile_id)
        generate_s1_ard_from_pids(args.s1_prd_ids, args.s2_tile_id,
            args.out_dirpath, dem_dirpath=args.dem_dirpath, working_dirpath_root=args.working_dirpath,
            clean=args.no_clean, upload_outputs=args.no_upload,
            data_source=args.data_source, dem_source=args.dem_source)
        logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!", args.s1_prd_ids, args.s2_tile_id)

    elif args.subparser_name == "wp":
        logger.debug("Starting Generate S1 ARD for the workplan %s ...", args.work_plan)
        generate_s1_ard_wp(args.work_plan, args.out_dirpath,
            args.dem_dirpath, args.working_dirpath,
            upload_outputs=args.upload)
        logger.info("Generation of the EWoC workplan %s for S1 part is ended!", args.work_plan)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
