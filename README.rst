=======
ewoc_s1
=======


The ewoc_s1 python package provide the python cli and API to process Sentinel-1 to the EWoC ARD format.

Description
===========

To generate EWoC ARD, the ewoc_s1 CLI perform the following tasks:

* Read or retrieve inputs S1 product ID from CLI arguments or from the worplan file
* Retrieve the SRTM 1s data from `ESA Website <http://step.esa.int/auxdata/dem/SRTMGL1/>`_ or creodias data bucket or local ewoc bucket 
* Download the S1 product ID from
  * creodias s3 bucket
  * from api thanks to `EODAG <https://eodag.readthedocs.io/en/stable/#>`_ and `ewoc_dag <https://github.com/WorldCereal/ewoc_dataship>`_ if needed
* Perform S1 calibration and projection to S2 grid thanks to `S1Tiling <https://gitlab.orfeo-toolbox.org/s1-tiling/s1tiling>`_ 
* Perform the same operation with thermal noise removal deactivated to identify no acquisition data and to follow snap convention about thermal noise
* Format to EWoC ARD format
* Upload to EWoC ARD s3 bucket

Installation
============
TODO

Usage
=====

The package provides one CLI command:

* *ewoc_s1_generate_s1_ard* which have two sub-commands:

.. code-block:: bash

    ewoc_generate_s1_ard -h

 * sub command *prd_ids* allow to run the processing on a list of S1 product ID 

.. code-block:: bash

    ewoc_generate_s1_ard prd_ids -h

    ewoc_generate_s1_ard prd_ids s2_tile_id \
                             S1_PRD_ID_1 \
                             S1_PRD_ID_2

 * sub command *wp* which allow to run the processing with input a EWoC workplan

.. code-block:: bash

    ewoc_generate_s1_ard wp -h

    ewoc_generate_s1_ard_wp /path/to/workplan.json

To download and upload data you need to configure some env variable from *ewoc_dag*
 as described `here <https://github.com/WorldCereal/ewoc_dataship#usage>`.
