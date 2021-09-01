=======
ewoc_s1
=======


The ewoc_s1 python package provide the python cli and API to process Sentinel-1 to the EWoC ARD format.

Description
===========

To generate EWoC ARD, the ewoc_s1 CLI perform the following tasks:

* Read or retrieve inputs S1 product ID from CLI arguments or from the worplan file or from database
* Retrieve the SRTM 1s data from `ESA Website <http://step.esa.int/auxdata/dem/SRTMGL1/>`_ or creodias data bucket or local ewoc bucket 
* Download the S1 product ID from
  * creodias s3 bucket
  * creodias finder api thanks to `EODAG <https://eodag.readthedocs.io/en/stable/#>`_ and `ewoc_dag <https://github.com/WorldCereal/ewoc_dataship>`_ if needed
* Perform S1 calibration and projection to S2 grid thanks to `S1Tiling <https://gitlab.orfeo-toolbox.org/s1-tiling/s1tiling>`_ 
* Perform the same operation with thermal noise removal deactivated to identify no acquisition data and to follow snap convention about thermal noise
* Format to EWoC ARD format
* Push to a s3 bucket if needed

The package provides 3 different commands:

* *ewoc_s1_generate_s1_ard_pid* which allow to run the processing on a list of S1 product ID

.. code-block:: bash

    ewoc_generate_s1_ard_pid -h

    ewoc_generate_s1_ard_pid s2_tile_id \
                             /path/to/output/dir \
                             S1_PRD_ID_1 \
                             S1_PRD_ID_2 \
                             --dem_dirpath /path/to/srtm/dir -w /path/to/working/dir -v

* *ewoc_generate_s1_ard_wp* which allow to run the processing with input a EWoC workplan

.. code-block:: bash

    ewoc_generate_s1_ard_wp -h

    ewoc_generate_s1_ard_wp /path/to/workplan.json \
                            /path/to/output/dir \
                            -v

* **Not currently implemented!** *ewoc_generate_s1_ard_db* which allow to run the processing with interaction with a database of S1 product to run

.. code-block:: bash

    ewoc_generate_s1_ard_db -h


To retrieve data from the creodias finder, ewoc_s1 CLI requests:

* a eodag.yaml file with creodias credentials
* or to set the following environements variables set with creodias credentials
 * EODAG__CREODIAS__AUTH__CREDENTIALS__USERNAME
 * EODAG__CREODIAS__AUTH__CREDENTIALS__PASSWORD

To push EWoC ARD to the desired s3 bucket, ewoc_s1 CLI requests to set the following variables:

* S3_ENDPOINT
* S3_ACCESS_KEY_ID
* S3_SECRET_ACCESS_KEY


.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
