
import argparse
import logging
from pathlib import Path
import sys
import shutil
import tempfile

from ewoc_dag.dag.srtm_dag import get_srtm1s

from ewoc_s1 import __version__
from ewoc_s1.generate_s1_ard import generate_s1_ard

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

logger = logging.getLogger(__name__)

def generate_s1_ard_from_pids(s1_prd_ids, s2_tile_id, out_dirpath_root,
                        dem_dirpath=None, working_dirpath_root=None,
                        clean=False, upload_outputs=False,
                        data_source='creodias_eodata', dem_source='creodias_eodata'):

    if working_dirpath_root is None:
        working_dirpath_root = Path(tempfile.gettempdir())

    working_dirpath = working_dirpath_root / 'ewoc_s1_pid'
    working_dirpath.mkdir(exist_ok=True)

    if dem_dirpath is None:
        dem_dirpath = working_dirpath / 'dem' / s2_tile_id
        dem_dirpath.mkdir(exist_ok=True, parents=True)
        try:
            get_srtm1s(s2_tile_id, dem_dirpath, source=dem_source)
        except:
            logger.critical('No elevation available!')
            return

    generate_s1_ard(s1_prd_ids, s2_tile_id, out_dirpath_root,
                    dem_dirpath, working_dirpath,
                    clean=clean, upload_outputs=upload_outputs,
                    data_source=data_source)

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
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path,
        default=Path(tempfile.gettempdir()))
    parser.add_argument("--clean", action='store_true', help= 'Clean all dirs')
    parser.add_argument("--upload", action='store_true', help= 'Upload outputs to s3 bucket')
    parser.add_argument("--data_source", dest="data_source", help= 'Source of the S1 input data', 
                        type=str,
                        default='creodias_eodata')
    parser.add_argument("--dem_source", dest="dem_source", help= 'Source of the DEM data', 
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
    generate_s1_ard_from_pids(args.s1_prd_ids, args.s2_tile_id,
                              args.out_dirpath, dem_dirpath=args.dem_dirpath, working_dirpath_root=args.working_dirpath,
                              clean=args.clean, upload_outputs=args.upload,
                              data_source=args.data_source, dem_source=args.dem_source)
    logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!", args.s1_prd_ids, args.s2_tile_id)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
