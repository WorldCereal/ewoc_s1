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
from datetime import datetime
import logging
from pathlib import Path
import sys

from dataship.dag.utils import get_product_by_id
from s1tiling.S1Processor import main as s1_process

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

class S1PrdIdInfo:

    FORMAT_DATETIME='%Y%m%dT%H%M%S'

    def __init__(self, s1_prd_id) -> None:
        # S1A_IW_GRDH_1SDV_20210708T060105_20210708T060130_038682_04908E_8979.SAFE
        # https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar/naming-conventions
        s1_prod_id_wsafe=s1_prd_id.split('.')[0]
        self._s1_prd_id=s1_prod_id_wsafe
        elt_prd_id = self._s1_prd_id.split('_')
        if len(elt_prd_id) == 9:
            self.mission_id = elt_prd_id[0]
            self.beam_mode = elt_prd_id[1]
            self.product_type = elt_prd_id[2][:3]
            self.resolution_class = elt_prd_id[2][3]
            self.processing_level = elt_prd_id[3][0]
            self.product_class = elt_prd_id[3][1]
            self.polarisation = elt_prd_id[3][2:]
            self.start_time = elt_prd_id[4]
            self.stop_time = elt_prd_id[5]
            self.absolute_orbit_number = elt_prd_id[6]
            self.mission_datatake_id = elt_prd_id[7]
            self.product_unique_id = elt_prd_id[8]
        else:
            raise ValueError('Sentinel 1 product id not provides the 9 keys values requested!')

    @property
    def product_unique_id(self):
        return self._product_unique_id

    @product_unique_id.setter
    def product_unique_id(self, value):
        if len(value) == 4:
            self._product_unique_id = value
        else:
            raise ValueError("Length of Product unique id different than 4 is not possible!")

    @property
    def mission_datatake_id(self):
        return self._mission_datatake_id

    @mission_datatake_id.setter
    def mission_datatake_id(self, value):
        if len(value) == 6:
            self._mission_datatake_id = value
        else:
            raise ValueError("Length of Mission datatake id different than 6 is not possible!")

    @property
    def absolute_orbit_number(self):
        return self._absolute_orbit_number

    @absolute_orbit_number.setter
    def absolute_orbit_number(self, value):
        if len(value) == 6:
            self._absolute_orbit_number = value
        else:
            raise ValueError("Length of Absolute orbit number different than 6 is not possible!", value)

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, value):
        self._start_time = datetime.strptime(value, self.FORMAT_DATETIME)

    @property
    def stop_time(self):
        return self._stop_time

    @stop_time.setter
    def stop_time(self, value):
        self._stop_time = datetime.strptime(value, self.FORMAT_DATETIME)
        
    @property
    def polarisation(self):
        return self._polarisation

    @polarisation.setter
    def polarisation(self, value):
        allowed_values= ['SH', 'SV', 'DH', 'DV']
        if value in allowed_values:
            self._polarisation = value
        else:
            raise ValueError("Polarisation different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def product_class(self):
        return self._product_class

    @product_class.setter
    def product_class(self, value):
        allowed_values = ['S', 'A']
        if value in allowed_values:
            self._product_class = value
        else:
            raise ValueError("Product Class different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def processing_level(self):
        return self._processing_level

    @processing_level.setter
    def processing_level(self, value):
        allowed_values = ['1', '2']
        if value in allowed_values:
            self._processing_level = value
        else:
            raise ValueError("Processing Level different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def resolution_class(self):
        return self._resolution_class

    @resolution_class.setter
    def resolution_class(self, value):
        allowed_values = ['F', 'H', 'M'] 
        if value in allowed_values:
            self._resolution_class = value
        else:
            raise ValueError("Resolution class different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def product_type(self):
        return self._mission_id

    @product_type.setter
    def product_type(self, value):
        allowed_values = ['SLC', 'GRD', 'OCN'] 
        if value in allowed_values:
            self._product_type = value
        else:
            raise ValueError("Product type different than " + ', '.join(allowed_values) + " is not possible!")

    @property
    def mission_id(self):
        return self._mission_id

    @mission_id.setter
    def mission_id(self, value):
        allowed_values= ['S1A', 'S1B']
        if value in allowed_values:
            self._mission_id = value
        else:
            raise ValueError("Mission ID different than "+ ', '.join(allowed_values) + " is not possible!")

    @property
    def beam_mode(self):
        return self._beam_mode

    @beam_mode.setter
    def beam_mode(self, value):
        allowed_values = ['IW', 'EW', 'WV']
        if value in allowed_values:
            self._beam_mode = value
        else:
            raise ValueError("Beam mode different than " + ', '.join(allowed_values) + " is not possible!")

    def __str__(self):
        return f'Info provided by the S1 product id are: mission_id={self.mission_id}, beam_mode={self.beam_mode}, \
product_type={self.product_type}, resolution_class={self.resolution_class}, \
processing_level={self.processing_level}, product_class={self.product_class}, \
polarisation={self.polarisation}, start time={self.start_time}, stop time={self.stop_time}, \
absolute_orbit_number={self.absolute_orbit_number}, mission_datatake_id={self._mission_datatake_id}, \
product_unique_id={self._product_unique_id}'

    def __repr__(self):
         return f'S1PrdIdInfo(s1_prd_id={self._s1_prd_id})'
      

def generate_s1_ard_id(s1_prd_ids, s2_tile_id, out_dirpath, conf_filepath,  dem_dirpath=None, working_dirpath_root=None):
    working_dirpath = working_dirpath_root / 'ewoc_s1'
    working_dirpath.mkdir(exist_ok=True)
    _logger.info('Product ids: %s', s1_prd_ids)
    _logger.info('s2_tile_id: %s', s2_tile_id)
    _logger.info('out_dirpath: %s', out_dirpath)
    _logger.info('dem_dirpath: %s', dem_dirpath)
    _logger.info('working_dirpath: %s', working_dirpath)

    s1_input_dir = working_dirpath /'input'
    s1_input_dir.mkdir(exist_ok=True)

    for s1_prd_id in s1_prd_ids:
        s1_prd_info = S1PrdIdInfo(s1_prd_id)
        _logger.info('%s', s1_prd_info)
        s1_prd_safe_dirpath = s1_input_dir / s1_prd_id
        s1_prd_wsafe_dirpath =  s1_input_dir / s1_prd_safe_dirpath.stem
        if not s1_prd_wsafe_dirpath.exists():
            get_product_by_id(s1_prd_id, s1_input_dir, 'creodias', config_file=conf_filepath)
            s1_prd_safe_dirpath.rename(s1_prd_wsafe_dirpath)
        else:
            _logger.info('S1 prd %s is already available on disk', s1_prd_id)

    config = configparser.ConfigParser()
    config['Paths'] = {'output': str(out_dirpath),
                       's1_images': str(s1_input_dir),
                       'srtm': str(dem_dirpath),
                       'tmp': str(working_dirpath)}

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

    s1_process.callback(20, False, False, False, False, False, config_filepath)

    to_ewoc_s1_ard( out_dirpath / s2_tile_id, out_dirpath, S1PrdIdInfo(s1_prd_ids[0]), s2_tile_id, rename_only=False)

    # TODO Push to bucket if requested

    return True


def to_ewoc_s1_ard(s1_process_output_dirpath,
                   out_dirpath,
                   s1_prd_info, 
                   s2_tile_id,
                   rename_only=False):
    
    orbit_direction = 'DES' # TODO retrieve from GDAL MTD of the output s1_process file or from mtd of the input product
    relative_orbit= 'TODO' # TODO retrieve from GDAL MTD of the output s1_process file or from mtd of the input product
    
    ewoc_output_dirname_elt = [s1_prd_info.mission_id,
                                s1_prd_info.start_time.strftime(S1PrdIdInfo.FORMAT_DATETIME),
                                orbit_direction,
                                relative_orbit,
                                s1_prd_info.absolute_orbit_number+s1_prd_info.mission_datatake_id+s1_prd_info.product_unique_id,
                                s2_tile_id]
    ewoc_output_dirname= '_'.join(ewoc_output_dirname_elt)
    ewoc_output_dirpath = out_dirpath / 'SAR' / s2_tile_id[:2] / s2_tile_id[2] / s2_tile_id[3:] / \
        str(s1_prd_info.start_time.year) / s1_prd_info.start_time.date().strftime('%Y%m%d') / ewoc_output_dirname
    #_logger.info('Create directory: %s', ewoc_output_dirpath)
    print('Create directory: %s', ewoc_output_dirpath)
    ewoc_output_dirpath.mkdir(exist_ok=True, parents=True)

    calibration_type = 'SIGMA0' # TODO retrieve from GDAL MTD of the output s1_process file or from parameters
    output_file_ext= '.tif'
    ewoc_output_filename_elt = ewoc_output_dirname_elt + [calibration_type]
    ewoc_output_filename_vv = '_'.join(ewoc_output_filename_elt + ['VV']) + output_file_ext
    ewoc_output_filepath_vv = ewoc_output_dirpath / ewoc_output_filename_vv
    print(ewoc_output_filename_vv)
    print(ewoc_output_filepath_vv)
    ewoc_output_filename_vh = '_'.join(ewoc_output_filename_elt + ['VH']) + output_file_ext
    ewoc_output_filepath_vh = ewoc_output_dirpath / ewoc_output_filename_vh
    print(ewoc_output_filename_vh)
    print(ewoc_output_filepath_vh)

    # TODO provide a more strict regex
    s1_process_output_filepath_vv = sorted(s1_process_output_dirpath.glob('*vv*.tif'))[0]
    s1_process_output_filepath_vh = sorted(s1_process_output_dirpath.glob('*vh*.tif'))[0]
    print(s1_process_output_filepath_vv)
    print(s1_process_output_filepath_vh)

    if rename_only:
        s1_process_output_filepath_vv.rename(ewoc_output_filepath_vv)
        s1_process_output_filepath_vh.rename(ewoc_output_filepath_vh)
    else:
        # TODO Convert to ewoc ARD raster format with db conversion
        # TODO manage the difference between 0 values and no data value (currently set to 0)
        ewoc_gdal_blocksize_20m = [512, 512]
        ewoc_gdal_compress_method = 'deflate'
        ewoc_gdal_dtype = 'uint16'
        ewoc_nodata = 0

        to_ewoc_s1_raster(s1_process_output_filepath_vv, ewoc_output_filepath_vv)
        to_ewoc_s1_raster(s1_process_output_filepath_vh, ewoc_output_filepath_vh)

def to_ewoc_s1_raster(s1_process_filepath, ewoc_filepath, blocksize=512, nodata_in=0, nodata_out=65535, compress=True):
    import otbApplication as otb

    app = otb.Registry.CreateApplication("BandMath")
    app.SetParameterStringList("il", [str(s1_process_filepath)])
    ewoc_output_filepath_vv_otb = str(ewoc_filepath)
    if nodata_in != nodata_out:
        ewoc_output_filepath_vv_otb += "?&nodata="+ str(nodata_out) 
    
    ewoc_output_filepath_vv_otb += "&gdal:co:TILED=YES" + \
        "&gdal:co:BLOCKXSIZE=" + str(blocksize) + \
            "&gdal:co:BLOCKYSIZE=" + str(blocksize)
    
    if compress:
        ewoc_output_filepath_vv_otb +="&gdal:co:COMPRESS=DEFLATE"

    _logger.debug(ewoc_output_filepath_vv_otb)
    app.SetParameterString("out", str(ewoc_output_filepath_vv_otb))
    app.SetParameterOutputImagePixelType("out", otb.ImagePixelType_uint16)

    otb_exp = "im1b1==" + str(nodata_in) + "?" + str(nodata_out) + ":10.*((10.*log10(im1b1)+83.)/20.)"
    _logger.debug(otb_exp)
    app.SetParameterString("exp", otb_exp)
    
    app.ExecuteAndWriteOutput()

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
    _logger.debug("Starting Generate S1 ARD for %s over %s MGRS Tile ...", args.s1_prd_ids, args.s2_tile_id)
    generate_s1_ard_id(args.s1_prd_ids, args.s2_tile_id, args.out_dirpath, args.conf_filepath,
                            args.dem_dirpath, args.working_dirpath)
    _logger.info("Generation of S1 ARD for %s over %s MGRS Tile is ended!", args.s1_prd_ids, args.s2_tile_id)


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
