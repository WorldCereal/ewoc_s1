
import argparse
import logging
from pathlib import Path
import shutil
import sys
import tempfile

from dataship.dag.srtm_dag import get_srtm1s

from ewoc_s1 import __version__
from ewoc_s1.generate_s1_ard import generate_s1_ard
from ewoc_s1.utils import EwocWorkPlanReader

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def generate_s1_ard_wp(work_plan_filepath, out_dirpath_root,
                       dem_dirpath=None, working_dirpath_root=None,
                       clean=True, upload_outputs=True,
                       data_source='creodias_eodata', dem_source='creodias_eodata'):

    if working_dirpath_root is None:
        working_dirpath_root = Path(tempfile.gettempdir())

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
            dem_dirpath = working_dirpath / 'dem'
            dem_dirpath.mkdir(exist_ok=True, parents=True)
            try:
                get_srtm1s(s2_tile_id, dem_dirpath, source=dem_source)
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
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path,
        default=Path(tempfile.gettempdir()))
    parser.add_argument("--upload", action='store_true', help= 'Upload outputs to s3 bucket')

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
    logger.debug("Starting Generate S1 ARD for the workplan %s...", args.work_plan)
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
