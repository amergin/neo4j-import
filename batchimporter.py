#!/usr/bin/python
# ^ use only system python, not lineo4j's own python

import os
import sys
import random
import string
import datetime
import timeit
#from neo4j import GraphDatabase
import ConfigParser
import db_util
import re # regexp
import timeit

# globals
LABEL_DELIMITER = ':'
INFO_CALL_DELIMITER = ':'

def getBatchPath( config ):
	return config.get("batch_import", "batch_path")

def getBatchTSVPath( config ):
	return config.get("batch_import", "batch_tsv_path")

def getTSVEdgeFile( config ):
	return getBatchTSVPath(config) + "edges.tsv"

def getTSVNodeFile( config ):
	return getBatchTSVPath(config) + "nodes.tsv"

def getTSVPatientFile( config, label ):
	return getBatchTSVPath(config) + label + "_patients.tsv"

def getNodeIndexName( config, nodeType, label ):
	if nodeType not in ['GEXP', 'CNVR', 'METH', 'CLIN', 'GNAB','SAMP','MIRN','RPPA']:
		raise NotImplementedError( "Unexpected node type" )
	return label + "_i_n_" + nodeType

def getTSVNodeIndexFile( config, nodeType, label ):
	if( nodeType == 'GEXP' ):
		return getBatchTSVPath( config ) + label + "_node_index_gexp.tsv"
	if( nodeType == 'METH' ):
		return getBatchTSVPath( config ) + label + "_node_index_meth.tsv"
	if( nodeType == 'CLIN' ):
		return getBatchTSVPath( config ) + label + "_node_index_clin.tsv"
	if( nodeType == 'CNVR' ):
		return getBatchTSVPath( config ) + label + "_node_index_cnvr.tsv"
	if( nodeType == 'GNAB' ):
		return getBatchTSVPath( config ) + label + "_node_index_gnab.tsv"
	if( nodeType == 'SAMP' ):
		return getBatchTSVPath( config ) + label + "_node_index_samp.tsv"
	if( nodeType == 'MIRN' ):
		return getBatchTSVPath( config ) + label + "_node_index_mirn.tsv"
	if( nodeType == 'RPPA' ):
		return getBatchTSVPath( config ) + label + "_node_index_rppa.tsv"
	raise NotImplementedError( "Unexpected node type" )

def getTSVEdgeIndexFile( config, edgeType, label ):
	if( edgeType == 'DISTANCE' ):
		return getBatchTSVPath( config ) + label + "_edge_index_dist.tsv"
	if( edgeType == 'ASSOCIATION'):
		return getBatchTSVPath( config ) + label + "_edge_index_assoc.tsv"
	raise NotImplementedError( "Unexpected edge type" )

def getEdgeIndexName( config, edgeType, label ):
	if edgeType not in ['ASSOCIATION', 'DISTANCE']:
		raise NotImplementedError( "Unexpected node type" )
	return label + "_i_e_" + edgeType

def getMaxMemory( config ):
	return config.get("batch_import", "max_memory")

def getDBPath( config ):
	return config.get("batch_import","database_path")

def getMysqlDumps( config ):
	return config.get("batch_import","mysql_dumps")

def getLabels( config ):
	return config.get("mysql_configs","datalabels")

def getDumpEdgesFile( config, dslabel ):
	dump_path = getMysqlDumps(config)
	dump_edges = dump_path + dslabel + "_edges.tsv"
	return dump_edges

def getDumpNodesFile( config, dslabel ):
	dump_path = getMysqlDumps(config)
	dump_nodes = dump_path + dslabel + "_nodes.tsv"
	return dump_nodes

def getDumpPatientsFile( config, label ):
	dump_path = getMysqlDumps(config)
	dump_nodes = dump_path + label + "_patients.tsv"
	return dump_nodes

def getLineDict( header, line ):
	columns = line.strip("\n").split("\t")
	return dict( zip( header, columns ) )

def getEdgeIndexingEnabled( config ):
	str = config.get("batch_import", "index_edges").lower()
	if( str == 'true' ):
		return True
	elif( str == 'false' ):
		return False
	else:
		raise ValueError("Unsupported type in index_edges, please type either true or false")


#def getLineDict( headerColumns, headerTypes, line ):
#	columns = line.strip("\n").split("\t")
#	return dict( zip( headerColumns, zip( columns, headerTypes ) ) )

def getDataType( mysqlType ):
	if( mysqlType == 'varchar' or mysqlType == 'longtext' ):
		return 'string'
	elif( mysqlType == 'int' ):
		return 'int'
	elif( mysqlType == 'double' ):
		return 'double'
	raise NotImplementedError("Add support for other primitives")



class DatasetImporter( object ):
	def __init__(self, config ):
		self.labels = getLabels( config ).split(LABEL_DELIMITER)
		self.config = config
		self.nodeHash = dict()
		self.patientFiles = []

	def start(self):
		# mysql dump of features is the TSV node file
		self.createMysqlDumps()
		self.createPatientBarcodeTSV()
		self.createNodeFiles()
		self.createEdgeFiles()
		self.createNeoDB()
		self.createInfoNodes()


	# create raw mysql dumps
	def createMysqlDumps( self ):
		print "Creating MySQL dumps"
		config = self.config

		for dslabel in self.labels:
			print "Creating MySQL dumps for labelname '%s'" %(dslabel) 
			dump_edges = getDumpEdgesFile( config, dslabel )
			dump_nodes = getDumpNodesFile( config, dslabel )
			dump_patients = getDumpPatientsFile( config, dslabel )

			table_feat = dslabel + "_features"
			table_edge = "mv_" + dslabel + "_feature_networks"
			table_patient = dslabel + "_patients"
			
			mydb = db_util.getDBSchema(config) 
			myuser = db_util.getDBUser(config) 
			mypw = db_util.getDBPassword(config) 
			myhost = db_util.getDBHost(config) 
			myport = str( db_util.getDBPort(config) )

			# Get nodes
			if( not os.path.isfile( dump_nodes ) ):
				nodeColumnTypes = dict()
				columns = [ "alias", "type", "source", "label", "chr", \
				"start", "end", "strand", "label_desc", \
				"patient_values", "patient_values_mean", "quantile_val", "quantile", "gene_interesting_score" ]

				cursor = db_util.getCursor(config)
				for column in columns:
					rows = cursor.execute( 
						"select DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' AND COLUMN_NAME = '%s'" \
						% ( table_feat, column ) )
					result = cursor.fetchone()[0] #tuple -> [0]
					nodeColumnTypes[column] = getDataType( result )
				cursor.close()

				# pvalue AS pvalue__varchar => dump header columns format is like pvalue__varchar
				os.system( "mysql --host=%s --port=%s --user=%s --password=%s --database=%s --batch --raw \
					-e \" SELECT %s FROM %s;\" > %s"  % (myhost, myport, myuser, mypw, mydb, \
						", ".join(['%s AS %s__%s' % (key,key,value) for (key,value) in nodeColumnTypes.items() ] ), table_feat, dump_nodes) )
			else:
				print "MySQL dump file at %s exists, skipping." %(dump_nodes)

			# Get edges
			if( not os.path.isfile( dump_edges ) ):
				columns = ["pvalue", "importance", "correlation", "patientct", \
				"alias1", "alias2", "f1chr", "f1start", "f1end", "f2chr", "f2start", "f2end"]
				edgeColumnTypes = dict()

				cursor = db_util.getCursor(config)
				for column in columns:
					cursor.execute( "select DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' AND COLUMN_NAME = '%s'" \
						% ( table_edge, column ) )
					result = cursor.fetchone()[0] #tuple -> [0]
					edgeColumnTypes[column] = getDataType( result )
				cursor.close()

				os.system( "mysql --host=%s --port=%s --user=%s --password=%s --database=%s --batch --raw \
					-e \"select %s FROM %s;\" > %s" \
					% (myhost, myport, myuser, mypw, mydb, \
						", ".join(['%s AS %s__%s' % (key,key,value) \
							for (key,value) in edgeColumnTypes.items() ] ), table_edge, dump_edges ) )
			else:
				print "MySQL dump file at %s exists, skipping." %(dump_edges)

			# Get patient barcodes:
			if( not os.path.isfile( dump_patients ) ):
				columns = ["barcode"]
				patientColumnTypes = dict()

				cursor = db_util.getCursor(config)
				for column in columns:
					cursor.execute( "select DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s' AND COLUMN_NAME = '%s'" \
						% ( table_patient, column ) )
					result = cursor.fetchone()[0] #tuple -> [0]
					patientColumnTypes[column] = getDataType( result )
				cursor.close()

				os.system( "mysql --host=%s --port=%s --user=%s --password=%s --database=%s --batch --raw \
					-e \"select %s FROM %s;\" > %s" \
					% (myhost, myport, myuser, mypw, mydb, \
						", ".join(['%s AS %s__%s' % (key,key,value) \
							for (key,value) in patientColumnTypes.items() ] ), table_patient, dump_patients ) )
			else:
				print "MySQL dump file at %s exists, skipping." %(dump_patients)

		print "Finished creating MySQL dumps"


	def createNodeFiles( self ):
		print "Starting to create node files."
		config = self.config

		node_tsv_file = open( getTSVNodeFile( config ), 'w')
		firstDataset = True
		header = []

		# what columns to include to indices?
		indexAttributes = ['label', 'source', 'type', 'chr', 'alias', 'start', 'end']
		indexAttributes.sort()

		# key-val is alias - lineno
		nodeHash = dict()

		tsv_lineno = 0
		for datasetno, datalabel in enumerate(self.labels):
			node_file = open( getDumpNodesFile( config, datalabel ), 'r')

			gexp_index_file = getTSVNodeIndexFile( config, 'GEXP', datalabel )
			meth_index_file = getTSVNodeIndexFile( config, 'METH', datalabel )
			clin_index_file = getTSVNodeIndexFile( config, 'CLIN', datalabel )
			cnvr_index_file = getTSVNodeIndexFile( config, 'CNVR', datalabel )
			rppa_index_file = getTSVNodeIndexFile( config, 'RPPA', datalabel )
			gnab_index_file = getTSVNodeIndexFile( config, 'GNAB', datalabel )
			samp_index_file = getTSVNodeIndexFile( config, 'SAMP', datalabel )
			mirn_index_file = getTSVNodeIndexFile( config, 'MIRN', datalabel )


			gexp_file = open( gexp_index_file, 'w')
			meth_file = open( meth_index_file, 'w')
			clin_file = open( clin_index_file, 'w')
			cnvr_file = open( cnvr_index_file, 'w') 
			rppa_file = open( rppa_index_file, 'w') 
			gnab_file = open( gnab_index_file, 'w') 
			samp_file = open( samp_index_file, 'w') 
			mirn_file = open( mirn_index_file, 'w') 

			headerColumns = []
			headerTypes = []

			# read the whole raw mysql dump file
			for lineno, line in enumerate( node_file ):
				if (lineno == 0):
					#header:
					header = line.strip("\n").split("\t")
					for h in header:
						headerColumns.append( h.split("__")[0] )
						headerTypes.append( h.split("__")[1] )

					# write headers to index files
					gexp_file.write( "id" + "\t" )
					meth_file.write( "id" + "\t" )
					clin_file.write( "id" + "\t" )
					cnvr_file.write( "id" + "\t" )
					rppa_file.write( "id" + "\t" )
					gnab_file.write( "id" + "\t" )
					samp_file.write( "id" + "\t" )
					mirn_file.write( "id" + "\t" )

					headerDict = dict( zip(headerColumns, headerTypes) )
					for lineno, (col, primtype) in enumerate( sorted( headerDict.items() ) ):
						if firstDataset:
							node_tsv_file.write( col + ":" + primtype + "\t" )
						if col in indexAttributes:
							gexp_file.write( col )#":" + primtype )
							meth_file.write( col )#":" + primtype )
							clin_file.write( col )#":" + primtype )
							cnvr_file.write( col )#":" + primtype )
							rppa_file.write( col )
							gnab_file.write( col )
							samp_file.write( col )
							mirn_file.write( col )
							if( lineno != len( zip(headerColumns, headerTypes) )- 1 ):
								gexp_file.write( "\t" )
								meth_file.write( "\t" )
								clin_file.write( "\t" )
								cnvr_file.write( "\t" )
								rppa_file.write( "\t" )
								gnab_file.write( "\t" )
								samp_file.write( "\t" )
								mirn_file.write( "\t" )
							else:
								gexp_file.write("\n")
								meth_file.write("\n")
								clin_file.write("\n")
								cnvr_file.write("\n")
								rppa_file.write("\n")
								gnab_file.write("\n")
								samp_file.write("\n")
								mirn_file.write("\n")
								if firstDataset:
									node_tsv_file.write( "\n")
									firstDataset = False
					continue

				# node ids start from 1, zero is reference node
				tsv_lineno += 1
				lineDict = getLineDict( headerColumns, line )
				self.nodeHash[ str(datasetno) + "|" + lineDict['alias'] ] = tsv_lineno

				# check most usual NULL columns:
				for key, val in lineDict.iteritems():
					if (val == 'NULL'): #and headerTypes[key] != 'string'):
						lineDict[key] = ''

				# write the node TSV
				node_tsv_file.write( "\t".join( ['%s' %(value) for key, value in sorted( lineDict.items() ) ] ) + "\n" )

				source = lineDict['source']
				if( source == 'GEXP' ):
					gexp_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'METH' ):
					meth_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'CLIN' ):
					clin_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'CNVR' ):
					cnvr_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'RPPA' ):
					rppa_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'GNAB' ):
					gnab_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'SAMP' ):
					samp_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )
				elif( source == 'MIRN' ):
					mirn_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\n" )

			# dump file reading ends.
			gexp_file.close()
			meth_file.close()
			clin_file.close()
			cnvr_file.close()
			rppa_file.close()
			gnab_file.close()
			samp_file.close()
			mirn_file.close()
			#separate index files for each dataset^
		node_tsv_file.close()

		print "Node files created."

	def createEdgeFiles( self ):
		print "Starting to create edge files"
		config = self.config

		# are one of the regions contained within the other or the same region?
		def regionIsSubset(region1, region2):
			assert( len(region1) == 2 and len(region2) == 2 )
			return ( ( region1[0] >= region2[0] and region1[1] <= region2[1] ) or \
				( region2[0] >= region1[0] and region2[1] <= region1[1] ) )

		edgeName = "ASSOCIATION"
		edge_tsv_file = open( getTSVEdgeFile(config), 'w' )

		# what columns to include to indices?
		indexAttributes = ['pvalue', 'importance', 'correlation']
		indexAttributes.sort()

		# what attributes to include to edges?
		edgeAttributes = ['pvalue', 'importance', 'correlation']
		edgeAttributes.sort()

		tsv_lineno = 0
		firstDataset = True
		for datasetno, datalabel in enumerate(self.labels):
			print "Creating edge files for dataset '%s'" %(datalabel)

			edge_file = open( getDumpEdgesFile(config, datalabel), 'r' )

			dist_index_file = open( getTSVEdgeIndexFile(config, 'DISTANCE', datalabel), 'w')
			#self.indexFiles['edges'].append( dist_index_file )
			assoc_index_file = open( getTSVEdgeIndexFile(config, 'ASSOCIATION', datalabel), 'w')
			#self.indexFiles['edges'].append( assoc_index_file )

			headerColumns = []
			headerTypes = []


			for lineno, line in enumerate( edge_file ):
				if (lineno == 0):
					#header:
					header = line.strip("\n").split("\t")
					for h in header:
						headerColumns.append( h.split("__")[0] )
						headerTypes.append( h.split("__")[1] )

					# write headers to index files
					dist_index_file.write( "id" + "\t" )
					assoc_index_file.write( "id" + "\t" )

					headerDict = dict( zip(headerColumns, headerTypes) )

					if firstDataset:
						edge_tsv_file.write( "start" + "\t" + "end" + "\t" + "type" + "\t" \
							+ "\t".join( [ key + ":" + headerDict[key] for key in edgeAttributes ] ) + "\t" + "distance:int" + "\n" )
						firstDataset = False

					for lineno, (col, primtype) in enumerate( sorted( headerDict.items() ) ):
						#edge_tsv_file.write( col + ":" + primtype + "\t" )
						if col in indexAttributes:
							dist_index_file.write( col )#+ ":" + primtype )
							assoc_index_file.write( col )#+ ":" + primtype )
							if( lineno != len( zip(headerColumns, headerTypes)) - 1 ):
								dist_index_file.write( "\t" )
								assoc_index_file.write( "\t" )
							else:
								assoc_index_file.write("\n")
					dist_index_file.write("\tdistance\n")
					continue	

				lineDict = getLineDict( headerColumns, line )

				# start and stop node indexes:
				start = str( self.nodeHash.get( str(datasetno) + "|" + lineDict.get('alias1') ) ) #columns[0] ) )
				stop = str( self.nodeHash.get( str(datasetno) + "|" + lineDict.get('alias2' ) ) )#columns[1] ) )
				f1chr = re.findall( r'\d+', lineDict.get('f1chr') )
				f2chr = re.findall( r'\d+', lineDict.get('f2chr') )
				distance = None

				# calculate chromosomal region distance
				if( len(f1chr) != 0 and len(f2chr) != 0 ):
					try:
						f1chr = int( f1chr[0] )
						f2chr = int( f2chr[0] )
						if( f1chr <= 24 and f2chr <= 24 and f1chr == f2chr ):
							f2start = int( lineDict['f2start'] )
							f2end = int( lineDict['f2end'] )
							f1start = int( lineDict['f1start'] )
							f1end = int( lineDict['f1end'] )

							f1 = [ f1start, f1end ]
							f2 = [ f2start, f2end ]
							f1.sort()
							f2.sort()

							if( ( f1[0] < f2[1] < f1[1] ) or ( f1[0] < f2[0] < f1[1] ) ):
								# regions overlap
								#print "DIST=0: f1,f2"
								#print f1,f2
								distance = 0
							else:
								if( regionIsSubset(f1,f2) ):
									#print "SUBSET:"
									#print f1,f2
									distance = 0
								else:
									distance = sorted( [ abs( f2[0] - f1[1] ), abs( f1[0] - f2[1] ) ] )[0]
					except ValueError:
						pass

				if distance:
					edge_tsv_file.write( start + "\t" + stop + "\t" + edgeName + "\t" + \
						"\t".join( [ lineDict[i] for i in edgeAttributes ] ) + "\t" + str(distance) + "\n" )
					#edge_tsv_file.write( start + "\t" + stop + "\t" + edgeName + "\t" + str(distance) \
					#	+ "\t" + "\t".join( ['%s' %(value) for key, value in sorted( lineDict.items() ) ] ) + "\n" )

					# for relationships the indexing starts from 0, not 1!
					dist_index_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\t" + str(distance) + "\n" )
					assoc_index_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\t" + str(distance) + "\n" )
				else:
					# distance column is now empty!
					edge_tsv_file.write( start + "\t" + stop + "\t" + edgeName + "\t" \
						+ "\t".join( [ lineDict[i] for i in edgeAttributes ] ) + "\t" + "" + "\t" + "\n" )
					assoc_index_file.write( str(tsv_lineno) + "\t" + "\t".join( [ lineDict[i] for i in indexAttributes ] ) + "\t" + "" + "\t" + "\n" )
				# edge ids start from zero
				tsv_lineno += 1

			edge_file.close()
			dist_index_file.close()
			assoc_index_file.close()
		edge_tsv_file.close()
		print "Edge files created."

	def createNeoDB( self ):
		config = self.config
		print "Creating Neo4j Database"
		print "------------------------------------------"
		# EXAMPLE:
		# java -server -Xmx4G -jar ../batch-import/target/batch-import-jar-with-dependencies.jar neo4j/data/graph.db \
		# nodes.csv rels.csv node_index users fulltext nodes_index.csv rel_index worked exact rels_index.csv
		command = str( "java -server -Xmx" + getMaxMemory(config) + " -jar " + getBatchPath(config) + " " + getDBPath( config ) \
			+ " " + getTSVNodeFile(config) + " " + getTSVEdgeFile(config) )

		for dataset in self.labels:
			command += " node_index " + " " + getNodeIndexName( config, 'GEXP', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'GEXP', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'METH', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'METH', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'CLIN', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'CLIN', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'CNVR', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'CNVR', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'RPPA', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'RPPA', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'GNAB', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'GNAB', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'MIRN', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'MIRN', dataset) + \
			" node_index " + " " + getNodeIndexName( config, 'SAMP', dataset) + " fulltext " + getTSVNodeIndexFile( config, 'SAMP', dataset) 
			if( getEdgeIndexingEnabled( config ) ):
				command += " rel_index " + " " + getEdgeIndexName( config, 'ASSOCIATION', dataset) + " fulltext " + getTSVEdgeIndexFile( config, 'ASSOCIATION', dataset)
				command += " rel_index " + " " + getEdgeIndexName( config, 'DISTANCE', dataset) + " fulltext " + getTSVEdgeIndexFile( config, 'DISTANCE', dataset)
		os.system(command)
		print "------------------------------------------"
		print "Finished creating Neo4j Database"

	def createInfoNodes( self ):
		command = "python create_info_nodes.py " + getDBPath(config) \
			+ " " + INFO_CALL_DELIMITER.join( self.labels ) + " " + INFO_CALL_DELIMITER.join( self.patientFiles )
		os.system( command )

	def createPatientBarcodeTSV( self ):
		print "Creating the Patient Barcode TSV files"
		config = self.config
		for datasetno, datalabel in enumerate(self.labels):
			patients_fileName = getDumpPatientsFile( config, datalabel )
			self.patientFiles.append( patients_fileName )

			patients_file = open( patients_fileName, 'r' )
			tsv_file = open( getTSVPatientFile(config, datalabel), 'w' )

			# first line is header
			patients_header = patients_file.readline()
			barcodes = patients_file.readline()

			tsv_file.write( patients_header )
			tsv_file.write( barcodes )
			tsv_file.close()
			patients_file.close()
		print "Finished creating Patient Barcode TSV files"

	def getLabelNumber(self):
		return len(self.labels)


def checkImportProgram(config):
	jarPath = getBatchPath(config)
	if not os.access( jarPath, os.R_OK ):
		print "Could not open JAR file at %s, check setting 'batch_path' in config. EXIT" %(jarPath)
		sys.exit(-1)


if __name__ == "__main__":
	if( len( sys.argv ) is not 2 ):
		print "Usage is py2.6 neo4j_csv.py batch_import.config"
		sys.exit(-1)

	config_file = sys.argv[1]
	if not os.access( config_file, os.R_OK ):
		print "Could not open config file. EXIT"
		sys.exit(-1)

	config = db_util.getConfig( config_file )
	checkImportProgram(config)

	if not os.path.exists( getBatchTSVPath( config ) ):
		print "TSV directory does not exist, creating."
		os.makedirs( getBatchTSVPath( config ) )

	if not os.path.exists( getMysqlDumps( config ) ):
		print "Dump directory does not exist, creating"
		os.makedirs( getMysqlDumps( config ) )

	print "Data import started at %s" %( str(datetime.datetime.now()) )

	importer = DatasetImporter( config )
	t = timeit.Timer(importer.start, 'gc.enable()')
	importTime = t.timeit(1)
	print "Data import ended at %s" %( str(datetime.datetime.now()) )
	print "Import time %0.2f seconds, %i datasets" %(importTime, importer.getLabelNumber() )
	sys.exit(0)