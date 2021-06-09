"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         fibonacci = ewoc_s1.skeleton:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``fibonacci`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This skeleton file can be safely removed if not needed!

References:
    - https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""

import argparse
import configparser
import logging
import os
import sys

from pathlib import Path

from distributed.scheduler import KilledWorker
from dask.distributed import Client, LocalCluster

from s1tiling.S1Processor import clean_logs, setup_worker_logs, check_tiles_to_process, extract_tiles_to_process, process_one_tile, check_srtm_tiles
from s1tiling.libs.S1FileManager import S1FileManager
from s1tiling.libs import Utils
from s1tiling.libs.configuration import Configuration
from s1tiling.libs.otbpipeline import FirstStep, PipelineDescriptionSequence
from s1tiling.libs.otbwrappers import AnalyseBorders, Calibrate, CutBorders, OrthoRectify, Concatenate, BuildBorderMask, SmoothBorderMask
from s1tiling.libs import exits

from ewoc_s1 import __version__

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


# ---- Python API ----
# The functions defined in this section can be imported by users in their
# Python scripts/interactive interpreter, e.g. via
# `from ewoc_s1.skeleton import fib`,
# when using this Python module as a library.




def generate_s1_ard(s1_prd_dir, s2_tile_id, out_dirpath,  dem_dirpath=None, working_dirpath=None):
    _logger.info('Product dir: %s', s1_prd_dir)
    _logger.info('s2_tile_id: %s', s2_tile_id)
    _logger.info('out_dirpath: %s', out_dirpath)
    _logger.info('dem_dirpath: %s', dem_dirpath)
    _logger.info('working_dirpath: %s', working_dirpath)

    config = configparser.ConfigParser()
    config['Paths'] = {'output': str(out_dirpath),
                       's1_images': str(s1_prd_dir),
                       'srtm': str(dem_dirpath),
                       'tmp': str(working_dirpath)}

    nb_s1_image_by_product = 2
    nb_cpu = os.cpu_count()
    print(nb_cpu)

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

    config = Configuration(config_filepath)
    os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(config.OTBThreads)

    with S1FileManager(config) as s1_file_manager:
        tiles_to_process = extract_tiles_to_process(config, s1_file_manager)
        if len(tiles_to_process) == 0:
            _logger.critical("No existing tiles found, exiting ...")
            sys.exit(exits.NO_S2_TILE)

        tiles_to_process_checked, needed_srtm_tiles = check_tiles_to_process(tiles_to_process, s1_file_manager)

        _logger.info("%s images to process on %s tiles",
                s1_file_manager.nb_images, tiles_to_process_checked)

        if len(tiles_to_process_checked) == 0:
            _logger.critical("No tiles to process, exiting ...")
            sys.exit(exits.NO_S1_IMAGE)

        _logger.info("Required SRTM tiles: %s", needed_srtm_tiles)

        if not check_srtm_tiles(config, needed_srtm_tiles):
            _logger.critical("Some SRTM tiles are missing, exiting ...")
            sys.exit(exits.MISSING_SRTM)

        if not os.path.exists(config.GeoidFile):
            _logger.critical("Geoid file does not exists (%s), exiting ...", config.GeoidFile)
            sys.exit(exits.MISSING_GEOID)

        # Prepare directories where to store temporary files
        # These directories won't be cleaned up automatically
        S1_tmp_dir = os.path.join(config.tmpdir, 'S1')
        os.makedirs(S1_tmp_dir, exist_ok=True)

        config.tmp_srtm_dir = s1_file_manager.tmpsrtmdir(needed_srtm_tiles)

        pipelines = PipelineDescriptionSequence(config)
        pipelines.register_pipeline([AnalyseBorders, Calibrate, CutBorders, OrthoRectify], 'FullOrtho', product_required=False)
        pipelines.register_pipeline([Concatenate],                                              product_required=True)
        if config.mask_cond:
            pipelines.register_pipeline([BuildBorderMask, SmoothBorderMask], 'GenerateMask',    product_required=True)


        clean_logs(config.log_config, config.nb_procs)
        cluster = LocalCluster(threads_per_worker=1, processes=True, n_workers=config.nb_procs, silence_logs=False)
        client = Client(cluster)
        client.register_worker_callbacks(lambda dask_worker: setup_worker_logs(config.log_config, dask_worker))

        log_level = lambda res: logging.INFO if bool(res) else logging.WARNING
        results = []
        for idx, tile_it in enumerate(tiles_to_process_checked):
            with Utils.ExecutionTimer("Processing of tile " + tile_it, True):
                res = process_one_tile(
                        tile_it, idx, len(tiles_to_process_checked),
                        s1_file_manager, pipelines, client,
                        searched_items_per_page=20)
                results += res

        nb_error_detected = 0
        for res in results:
            if not bool(res):
                nb_error_detected += 1

        if nb_error_detected > 0:
            _logger.warning('Execution report: %s errors detected', nb_error_detected)
        else:
            _logger.info('Execution report: no error detected')

        if results:
            for res in results:
                _logger.log(log_level(res), ' - %s', res)
        else:
            _logger.info(' -> Nothing has been executed')

        if nb_error_detected > 0:
            sys.exit(exits.TASK_FAILED)


    
    return True


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
    parser = argparse.ArgumentParser(description="Generate EWoC S1 ARD from S1 Product ARD over S2 Tile Id")
    parser.add_argument(
        "--version",
        action="version",
        version="ewoc_s1 {ver}".format(ver=__version__),
    )
    parser.add_argument(dest="s1_prd_dir", help="Sentinel-1 Product directory", type=Path)
    parser.add_argument(dest="s2_tile_id", help="Sentinel-2 Tile ID", type=str)
    parser.add_argument(dest="out_dirpath", help="Output Dirpath", type=Path)
    parser.add_argument("--dem_dirpath", dest="dem_dirpath", help="DEM dirpath", type=Path)
    parser.add_argument("-w", dest="working_dirpath", help="Working dirpath", type=Path)
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
    _logger.debug("Starting Generate S1 ARD for %s over %s MGRS Tile ...", args.s1_prd_dir, args.s2_tile_id)
    generate_s1_ard(Path(args.s1_prd_dir), args.s2_tile_id, args.out_dirpath,  args.dem_dirpath, args.working_dirpath)
    _logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!", args.s1_prd_dir, args.s2_tile_id)


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
