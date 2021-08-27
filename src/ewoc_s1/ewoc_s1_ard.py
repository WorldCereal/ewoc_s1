
import os
import logging

from ewoc_s1 import __version__
from ewoc_s1.s1_prd_id import S1PrdIdInfo

import otbApplication as otb

__author__ = "Mickael Savinaud"
__copyright__ = "Mickael Savinaud"
__license__ = "Apache v2"

logger = logging.getLogger(__name__)

def to_ewoc_s1_ard(s1_process_output_dirpath,
                   out_dirpath,
                   s1_prd_info,
                   s2_tile_id,
                   rename_only=False,
                   clean_input_file=False):

    # TODO provide a more strict regex
    s1_process_output_filepath_vv = sorted(s1_process_output_dirpath.glob('*vv*.tif'))[0]
    s1_process_output_filepath_vh = sorted(s1_process_output_dirpath.glob('*vh*.tif'))[0]

    relative_orbit = 'TODO' # TODO retrieve from GDAL MTD of the output s1_process file or from mtd of the input product
    calibration_type = 'SIGMA0' # TODO retrieve from GDAL MTD of the output s1_process file or from parameters

    orbit_direction = 'DES'
    if os.path.basename(s1_process_output_filepath_vv).find('_ASC_') != -1:
        orbit_direction = 'ASC'

    ewoc_output_dirname_elt = [s1_prd_info.mission_id,
                                s1_prd_info.start_time.strftime(S1PrdIdInfo.FORMAT_DATETIME),
                                orbit_direction,
                                relative_orbit,
                                s1_prd_info.absolute_orbit_number+s1_prd_info.mission_datatake_id+s1_prd_info.product_unique_id,
                                s2_tile_id]
    ewoc_output_dirname= '_'.join(ewoc_output_dirname_elt)
    ewoc_output_dirpath = out_dirpath / 'SAR' / s2_tile_id[:2] / s2_tile_id[2] / s2_tile_id[3:] / \
        str(s1_prd_info.start_time.year) / s1_prd_info.start_time.date().strftime('%Y%m%d') / ewoc_output_dirname
    logger.debug('Create output directory: %s', ewoc_output_dirpath)
    ewoc_output_dirpath.mkdir(exist_ok=True, parents=True)

    output_file_ext= '.tif'
    ewoc_output_filename_elt = ewoc_output_dirname_elt + [calibration_type]
    ewoc_output_filename_vv = '_'.join(ewoc_output_filename_elt + ['VV']) + output_file_ext
    ewoc_output_filepath_vv = ewoc_output_dirpath / ewoc_output_filename_vv
    logger.debug('Output VV filepath: %s', ewoc_output_filepath_vv)
    ewoc_output_filename_vh = '_'.join(ewoc_output_filename_elt + ['VH']) + output_file_ext
    ewoc_output_filepath_vh = ewoc_output_dirpath / ewoc_output_filename_vh
    logger.debug('Output VH filepath: %s', ewoc_output_filepath_vh)

    if rename_only:
        s1_process_output_filepath_vv.rename(ewoc_output_filepath_vv)
        s1_process_output_filepath_vh.rename(ewoc_output_filepath_vh)
    else:
        # TODO manage the difference between 0 values and no data value (currently set to 0)
        ewoc_gdal_blocksize_20m = [512, 512]
        ewoc_gdal_compress_method = 'deflate'
        ewoc_gdal_dtype = 'uint16'
        ewoc_nodata = 0

        to_ewoc_s1_raster(s1_process_output_filepath_vv, ewoc_output_filepath_vv, nodata_in=65535, nodata_out=65535)
        to_ewoc_s1_raster(s1_process_output_filepath_vh, ewoc_output_filepath_vh, nodata_in=65535, nodata_out=65535)

        if clean_input_file:
            s1_process_output_filepath_vv.unlink()
            s1_process_output_filepath_vh.unlink()

def to_ewoc_s1_raster(s1_process_filepath, ewoc_filepath, blocksize=512, nodata_in=0, nodata_out=0, compress=True):

    s1_process_noized_dirpath = os.path.abspath(os.path.join(os.path.dirname(s1_process_filepath),"../../s1process_noized/"))
    s1_process_noized_filepath = os.path.join(s1_process_noized_dirpath, os.path.basename(os.path.dirname(s1_process_filepath)))
    s1_process_noized_filepath = os.path.join(s1_process_noized_filepath, os.path.basename(s1_process_filepath))

    msk = otb.Registry.CreateApplication("BandMath")
    msk.SetParameterStringList("il", [str(s1_process_filepath), str(s1_process_noized_filepath)])
    msk.SetParameterString("out", str(s1_process_filepath))
    mask_exp = "im2b1==0?" + str(nodata_out) + ":im1b1"
    msk.SetParameterString("exp", mask_exp)
    msk.ExecuteAndWriteOutput()

    app = otb.Registry.CreateApplication("BandMath")
    app.SetParameterStringList("il", [str(s1_process_filepath)])
    ewoc_output_filepath_vv_otb = str(ewoc_filepath) + '?'
    #    if nodata_in != nodata_out:
    ewoc_output_filepath_vv_otb += "&nodata="+ str(nodata_out)

    ewoc_output_filepath_vv_otb += "&gdal:co:TILED=YES" + \
        "&gdal:co:BLOCKXSIZE=" + str(blocksize) + \
            "&gdal:co:BLOCKYSIZE=" + str(blocksize)

    if compress:
        ewoc_output_filepath_vv_otb +="&gdal:co:COMPRESS=DEFLATE"

    logger.debug(ewoc_output_filepath_vv_otb)
    app.SetParameterString("out", str(ewoc_output_filepath_vv_otb))
    app.SetParameterOutputImagePixelType("out", otb.ImagePixelType_uint16)
    otb_exp = "im1b1==0?0:im1b1==" + str(nodata_out) + "?" + str(nodata_out) + ":10.^((10.*log10(im1b1)+83.)/20.)"
    logger.debug(otb_exp)
    app.SetParameterString("exp", otb_exp)

    app.ExecuteAndWriteOutput()
