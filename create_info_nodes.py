# -*- coding: utf-8 -*-
#!/home/lineo4j/python/bin/python

# mundane modification stuff
import os
import commands
import sys
import datetime
import re # regexp
import time

# use python-embedded to import these few nodes:
from neo4j import GraphDatabase

class MetaInfoCreator( object ):
	def __init__( self, databasePath, datasetDict):

		#increment to existing db
		self.db = GraphDatabase( databasePath )
		self.datasetDict = datasetDict

	def start(self):
		self._createInfoNodes()
		self._finish()

	def _createInfoNodes( self ):
		print "Creating info nodes"
		# do all insertions within one transaction: complete failure or success!
		with self.db.transaction:
			metaVertex = self.db.node() #self.db.reference_node
			print "Meta node created, id %i" %( metaVertex.id )
			index = self.db.node.indexes.create('meta')
			index['meta']['meta'] = metaVertex

			for num, (label, patientFile) in enumerate( self.datasetDict.items() ):
				patientFile = open( patientFile, 'r')
				patientFile.readline() #header

				datasetVertex = self.db.node()
				datasetVertex['datalabel'] = label
				datasetVertex['barcode'] = patientFile.readline().strip("\n")

				metaVertex.relationships.create('DATASET', datasetVertex, label=label)
				patientFile.close()

	def _finish(self):
		self.db.shutdown()
		print "Infonodes created" 

if __name__ == "__main__":
	if( len( sys.argv ) is not 4 ):
		print "Usage is python databasePath datasetlabel1:datasetlabel2:..:datasetlabelN patientFile1:patientFile2:...:patientFileN"
		sys.exit(-1)



	dbPath = sys.argv[1]
	datasetLabels = sys.argv[2].split(":")
	patientFiles = sys.argv[3].split(":")
	if not os.path.exists( dbPath ):
		print "Database directory does not exist, will not create empty db."
		sys.exit(-1)

	creator = MetaInfoCreator( dbPath, dict(zip( datasetLabels, patientFiles ) ) )
	creator.start()

	sys.exit(0)