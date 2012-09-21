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