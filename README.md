# MTDDB
Software for handling the construction DB

**Browsing data**
=============
Data can be browsed using CMSDCADEV at https://cmsdcadev.cern.ch/

**Using the CMSDCADEV browser outside CERN**
========================================
Look at https://security.web.cern.ch/recommendations/en/ssh_browsing.shtml

**btl_definition-v2.py**
====================
This script is used to create the SQL statements needed to define parts and attributes in the
DB. It still has to be optimised, given that the statements are not yet in the right order
(some of them depend on others). Nevertheless, it currently generates the SQL statements correctly.

**mtdConstructionDBTools**
======================
This is a directory where the libraries used in current applications are stored

**registerBatchIngot.py**
=====================
Script used to generate the XML code to register a batch (to be updated)

**registerSingleXtal.py**
=====================
Script used to generate the XML code to register single crystals (to be updated)

**registerMatrixBatch.py**
======================
Script used to generate the XML code to register LYSO matrices

**gen_create_condition_sql.pl**
===========================
This script generates the SQL command to create tables for conditions.
It uses ``btl_conditions_template.sql`` as a template.