INSTALLATION

Dependencies:
* latest version of Batch-import by Michael Hunger (https://github.com/jexp/batch-import)
	- set batch_path to point to the appropriate JAR
* Python packages
	- MySQLdb for db_util.py
	- Python bindings for Neo4j (https://github.com/neo4j/python-embedded) for storing meta nodes

RUNNING THE SCRIPT

1. Modify the batch_import.config to your needs
2. Run by issuing ./batchimporter.py batch_import.config
3. Once finished, copy the resulting db directory to you neo4j data directory

EXAMPLE RUN

bash-4.1$ ./batchimporter.py batch_import.config
Data import started at 2012-09-21 20:48:06.795819
Creating MySQL dumps
Creating MySQL dumps for labelname 'cbm_pc_quantrev_0706'
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/cbm_pc_quantrev_0706_nodes.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/cbm_pc_quantrev_0706_edges.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/cbm_pc_quantrev_0706_patients.tsv exists, skipping.
Creating MySQL dumps for labelname 'crc_31july'
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_31july_nodes.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_31july_edges.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_31july_patients.tsv exists, skipping.
Creating MySQL dumps for labelname 'brca_public_0613'
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/brca_public_0613_nodes.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/brca_public_0613_edges.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/brca_public_0613_patients.tsv exists, skipping.
Creating MySQL dumps for labelname 'crc_noroi_1807'
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_noroi_1807_nodes.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_noroi_1807_edges.tsv exists, skipping.
MySQL dump file at /home/lineo4j/import/gitrepo/dumps/crc_noroi_1807_patients.tsv exists, skipping.
Finished creating MySQL dumps
Creating the Patient Barcode TSV files
Finished creating Patient Barcode TSV files
Starting to create node files.
Node files created.
Starting to create edge files
Creating edge files for dataset 'cbm_pc_quantrev_0706'
Creating edge files for dataset 'crc_31july'
Creating edge files for dataset 'brca_public_0613'
Creating edge files for dataset 'crc_noroi_1807'
Edge files created.
Creating Neo4j Database
------------------------------------------
Using Existing Configuration File
..
Importing 204099 Nodes took 38 seconds 
.................................................................................................... 87686 ms for 10000000
.................................................................................................... 276035 ms for 10000000
...........
Importing 21134392 Relationships took 412 seconds 

Importing 29595 Done inserting into cbm_pc_quantrev_0706_i_n_GEXP Index took 10 seconds 

Importing 5000 Done inserting into cbm_pc_quantrev_0706_i_n_METH Index took 5 seconds 

Importing 18 Done inserting into cbm_pc_quantrev_0706_i_n_CLIN Index took 0 seconds 

Importing 19872 Done inserting into cbm_pc_quantrev_0706_i_n_CNVR Index took 1 seconds 

Importing 0 Done inserting into cbm_pc_quantrev_0706_i_n_RPPA Index took 0 seconds 

Importing 0 Done inserting into cbm_pc_quantrev_0706_i_n_GNAB Index took 0 seconds 

Importing 0 Done inserting into cbm_pc_quantrev_0706_i_n_MIRN Index took 0 seconds 

Importing 0 Done inserting into cbm_pc_quantrev_0706_i_n_SAMP Index took 0 seconds 

Importing 15399 Done inserting into crc_31july_i_n_GEXP Index took 4 seconds 

Importing 21166 Done inserting into crc_31july_i_n_METH Index took 1 seconds 

Importing 73 Done inserting into crc_31july_i_n_CLIN Index took 0 seconds 

Importing 9674 Done inserting into crc_31july_i_n_CNVR Index took 0 seconds 

Importing 171 Done inserting into crc_31july_i_n_RPPA Index took 0 seconds 

Importing 17256 Done inserting into crc_31july_i_n_GNAB Index took 1 seconds 

Importing 519 Done inserting into crc_31july_i_n_MIRN Index took 0 seconds 

Importing 164 Done inserting into crc_31july_i_n_SAMP Index took 0 seconds 

Importing 5485 Done inserting into brca_public_0613_i_n_GEXP Index took 0 seconds 

Importing 4982 Done inserting into brca_public_0613_i_n_METH Index took 0 seconds 

Importing 61 Done inserting into brca_public_0613_i_n_CLIN Index took 0 seconds 

Importing 3831 Done inserting into brca_public_0613_i_n_CNVR Index took 0 seconds 

Importing 165 Done inserting into brca_public_0613_i_n_RPPA Index took 0 seconds 

Importing 5482 Done inserting into brca_public_0613_i_n_GNAB Index took 0 seconds 

Importing 605 Done inserting into brca_public_0613_i_n_MIRN Index took 0 seconds 

Importing 315 Done inserting into brca_public_0613_i_n_SAMP Index took 0 seconds 

Importing 15399 Done inserting into crc_noroi_1807_i_n_GEXP Index took 1 seconds 

Importing 21166 Done inserting into crc_noroi_1807_i_n_METH Index took 1 seconds 

Importing 73 Done inserting into crc_noroi_1807_i_n_CLIN Index took 0 seconds 

Importing 9518 Done inserting into crc_noroi_1807_i_n_CNVR Index took 0 seconds 

Importing 171 Done inserting into crc_noroi_1807_i_n_RPPA Index took 0 seconds 

Importing 17256 Done inserting into crc_noroi_1807_i_n_GNAB Index took 0 seconds 

Importing 519 Done inserting into crc_noroi_1807_i_n_MIRN Index took 0 seconds 

Importing 164 Done inserting into crc_noroi_1807_i_n_SAMP Index took 0 seconds 

Total import time: 489 seconds 
------------------------------------------
Finished creating Neo4j Database
Creating info nodes
Meta node created, id 204100
Infonodes created
Data import ended at 2012-09-21 21:06:23.105750
Import time 1096.31 seconds, 4 datasets

bash-4.1$ du -csh targetdb/
2.9G    targetdb/
2.9G    total