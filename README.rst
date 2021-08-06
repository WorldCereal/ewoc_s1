=======
ewoc_s1
=======


The ewoc_s1 python package provide the python cli and API to process Sentinel-1 to the EWoC ARD format.

Description
===========

To generate EWoC ARD, ewoc_s1 cli performs the following tasks:

* Read or retrieve inputs S1 product ID from CLI arguments or from the worplan file or from database
* Retrieve the SRTM 1s data from `ESA Website <http://step.esa.int/auxdata/dem/SRTMGL1/>`_ 
* Download the S1 produc ID from creodias finder api thanks to `EODAG <https://eodag.readthedocs.io/en/stable/#>`_ and `ewoc_dag <https://github.com/WorldCereal/ewoc_dataship>`_ if needed
* Perform S1 calibration and projection to S2 grid thanks to `S1Tiling <https://gitlab.orfeo-toolbox.org/s1-tiling/s1tiling>`_ 
* Format to EWoC ARD format
* Push to a s3 bucket if needed

The package provides 3 different commands:

* ewoc_s1_generate_s1_ard_pid which allow to run the processing on a list of S1 product ID

.. code-block:: bash

    ewoc_s1_generate_s1_ard_pid -h

* ewoc_s1_generate_s1_ard_wp which allow to run the processing with input a EWoC workplan
* ewoc_s1_generate_s1_ard_db which allow to run the processing with interaction with a database of S1 product to run




.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
