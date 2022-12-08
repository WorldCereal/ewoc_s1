
import argparse
from datetime import datetime
import logging
from pathlib import Path
import sys
import shutil
from tempfile import gettempdir
from typing import Optional, List, Tuple

from ewoc_dag.srtm_dag import get_srtm_from_s2_tile_id, get_srtm_1s_default_provider
from ewoc_dag.s1_dag import get_s1_default_provider

from ewoc_s1 import EWOC_S1_DEM_DOWNLOAD_ERROR, EWOC_S1_UNEXPECTED_ERROR, __version__
from ewoc_s1.generate_s1_ard import S1ARDProcessorBaseError, generate_s1_ard
from ewoc_s1.utils import EwocWorkPlanReader

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

class S1DEMProcessorError(Exception):
    """Exception raised for errors in the S1 ARD generation at DEM download step."""

    def __init__(self, error):
        self._error = error
        self._message = "Error during DEM donwload:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} {self._error} !"

class S1ARDProcessorError(Exception):
    """Exception raised for errors in the S1 ARD generation."""

    def __init__(self, s2_tile_id, s1_prd_ids, s1_data_source, exit_code):
        self._s2_tile_id = s2_tile_id
        self._s1_prd_ids = s1_prd_ids
        self._s1_data_source = s1_data_source
        self.exit_code = exit_code
        self._message = "Error during S1 ARD generation:"
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message} No S1 ARD on {self._s2_tile_id} for {self._s1_prd_ids} from {self._s1_data_source} !"

def _get_default_prod_id()->str:
    str_now=datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"0000_000_{str_now}"

def generate_s1_ard_wp(work_plan_filepath:Path,
                       out_dirpath_root:Path=Path(gettempdir()),
                       working_dirpath_root=Path(gettempdir()),
                       clean:bool=True, upload_outputs:bool=True,
                       data_source:str=get_s1_default_provider(),
                       dem_source:str=get_srtm_1s_default_provider(),
                       production_id: Optional[str]=None):

    if production_id is None:
        logger.warning("Use computed production id but we must used the one in wp")
        production_id = _get_default_prod_id()
        logger.debug('production id: %s', production_id)

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

        if not Path(dem_source).is_dir():
            dem_dirpath = wd_dirpath_tile / 'dem'
            dem_dirpath.mkdir(exist_ok=True, parents=True)
            try:
                get_srtm_from_s2_tile_id(s2_tile_id, dem_dirpath, source=dem_source)
            except:
                logger.critical('No elevation available!')
                return
        else:
            logger.info('Use local directory for DEM!')
            dem_dirpath = Path(dem_source)

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


def generate_s1_ard_from_pids(s1_prd_ids:List[str], s2_tile_id:str,
                        out_dirpath_root:Path=Path(gettempdir()),
                        working_dirpath_root:Path=Path(gettempdir()),
                        clean:bool=True, upload_outputs:bool=True,
                        data_source:str=get_s1_default_provider(),
                        dem_source:str=get_srtm_1s_default_provider(),
                        production_id: Optional[str]=None)->Tuple[int, str]:
    """ Generate SAR ARD data from Sentinel-1 GRD products

    Args:
        s1_prd_ids (List[str]): List of Sentinel-1 products ID
        s2_tile_id (str): Sentinel-2 MGRS ID
        out_dirpath_root (Path, optional): Path where to wirte the SAR ARD data. Defaults to Path(gettempdir()).
        working_dirpath_root (Path, optional): Path where to write temporary data. Defaults to Path(gettempdir()).
        clean (bool, optional): Flag to indicate if you want clean directory or not. Defaults to True.
        upload_outputs (bool, optional): Flag to indicate if you want upload or not the products. Defaults to True.
        data_source (str, optional): Provide the source of Sentinel-1 GRD products. Defaults to get_s1_default_provider().
        dem_source (str, optional): Provide the source of DEM. Defaults to get_srtm_1s_default_provider().
        production_id (str, optional): Production ID. Defaults to None.

    Raises:
        S1DEMProcessorError: When error raise with the DEM retrieval
        S1ARDProcessorError: When error raise with S1 ARD processing

    Returns:
        Tuple[int, str]: return the number of files uploaded and the s3 path
    """
    if production_id is None:
        production_id=_get_default_prod_id()
        logger.debug('production id: %s', production_id)

    working_dirpath = working_dirpath_root / 'ewoc_s1_pid'
    working_dirpath.mkdir(exist_ok=True)

    if not Path(dem_source).is_dir():
        dem_dirpath = working_dirpath / 'dem' / s2_tile_id
        dem_dirpath.mkdir(exist_ok=True, parents=True)
        try:
            get_srtm_from_s2_tile_id(s2_tile_id,
                out_dirpath= dem_dirpath,
                source=dem_source, resolution='1s')
        except:
            logger.error('No elevation available!')
            raise S1DEMProcessorError(f'No elevation for {s2_tile_id} from {dem_source}')
    else:
        logger.info('Use local directory for DEM!')
        dem_dirpath = Path(dem_source)

    try:
        nb_s1_ard_files, s1_ard_s3path = generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath_root,
                        dem_dirpath, working_dirpath,
                        clean=clean, upload_outputs=upload_outputs,
                        data_source=data_source, production_id=production_id)
    except S1ARDProcessorBaseError as exc:
        logger.error(exc)
        raise S1ARDProcessorError(s2_tile_id, s1_prd_ids, data_source, exc.exit_code)
    finally:
        if clean:
            shutil.rmtree(working_dirpath)

    return nb_s1_ard_files, s1_ard_s3path

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args_cli:List[str]):
    """Parse command line parameters

    Args:
      args_cli (List[str]): command line parameters as list of strings
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
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path,
        default=Path(gettempdir()))

    parser.add_argument("--no-clean",
        action='store_false',
        help= 'Avoid to clean all dirs and files')
    parser.add_argument("--no-upload",
        action='store_false',
        help= 'Skip the upload of ard files to s3 bucket')

    parser.add_argument("--prod-id",
        dest="prod_id",
        help="Production ID that will be used to upload to s3 bucket, \
            by default it is computed internally")

    parser.add_argument("--data-source", dest="data_source", help= 'Source of the S1 input data',
                        type=str,
                        default=get_s1_default_provider())
    parser.add_argument("--dem-source", dest="dem_source", help= 'Source of the DEM data',
                        type=str,
                        default=get_srtm_1s_default_provider())
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
    parser_wp.add_argument(dest="work_plan",
        help="EWoC workplan in json format",
        type=Path)

    args = parser.parse_args(args_cli)

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

        logger.debug("Starting Generate S1 ARD for %s over %s MGRS Tile ...",
            args.s1_prd_ids, args.s2_tile_id)

        try:
            nb_s1_ard_files, s1_ard_s3path=generate_s1_ard_from_pids(
                args.s1_prd_ids, args.s2_tile_id,
                args.out_dirpath, working_dirpath_root=args.working_dirpath,
                clean=args.no_clean, upload_outputs=args.no_upload,
                data_source=args.data_source, dem_source=args.dem_source, production_id=args.prod_id)
        except S1DEMProcessorError as exc:
            logger.critical(exc)
            sys.exit(EWOC_S1_DEM_DOWNLOAD_ERROR)
        except S1ARDProcessorError as exc:
            logger.critical(exc)
            sys.exit(exc.exit_code)
        except BaseException as exc:
            logger.critical(f"Unexpected {exc=}, {type(exc)=}")
            sys.exit(EWOC_S1_UNEXPECTED_ERROR)
        else:
            logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!",
                args.s1_prd_ids, args.s2_tile_id)
            if args.no_upload:
                logger.info("S1 ARD product is available at %s",s1_ard_s3path)
                # TODO Remove print!
                print(f'Uploaded {nb_s1_ard_files} tif files to bucket | {s1_ard_s3path}')

    elif args.subparser_name == "wp":
        logger.debug("Starting Generate S1 ARD for the workplan %s ...", args.work_plan)
        generate_s1_ard_wp(args.work_plan, args.out_dirpath,
            args.working_dirpath,
            clean=args.no_clean, upload_outputs=args.no_upload,
            data_source=args.data_source, dem_source=args.dem_source,
            production_id=args.prod_id)
        logger.info("Generation of the EWoC workplan %s for S1 part is ended!", args.work_plan)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
