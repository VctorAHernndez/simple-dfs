# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: mds_db.py (comes with 7 other files)

# SQL support library. Database info for the metadata server



import sqlite3

class mds_db:

	def __init__(self, db_name):
		self.c = None
		self.db_name = db_name
		self.conn = None
	
	def Connect(self):
		"""Connect to the database file"""
		try:
			self.conn = sqlite3.connect(self.db_name)
			self.c = self.conn.cursor()
			self.conn.isolation_level = None
			return 1
		except:
			return 0

	def Close(self):
		"""Close cursor to the database"""
		try:
			self.conn.commit()
			self.c.close()
			self.conn.close()
			return 1
		except:
			return 0
	
	def AddDataNode(self, address, port):
		"""
			Adds new datanode to the metadata server
			(i.e. the information to 'connect' to the data node)
		"""
		query = """insert into dnode (address, port) values ("%s", %s)""" % (address, port)

		try:
			self.c.execute(query)
			return self.c.lastrowid 
		except sqlite3.IntegrityError as e:
			if e.message.split()[0].strip() == "UNIQUE":
				return 0
			else:
				raise
			
	def CheckNode(self, address, port):
		"""Check if datanode is in database and nid"""
		query = """select nid from dnode where address="%s" and port=%s""" % (address, port)
		try:
			self.c.execute(query)
			return self.c.fetchone()[0]
		except:
			return None

	def GetDataNodes(self):
		"""Returns a list of data node tuples (address, port)"""
		query = """select address, port from dnode where 1"""
		self.c.execute(query)
		return self.c.fetchall()

	def InsertFile(self, fname, fsize):
		"""Create the inode's attributes (i.e. the name of the file and its size)"""
		query = """insert into inode (fname, fsize) values ("%s", %s)""" % (fname, fsize)
		try:
			self.c.execute(query)
			return 1
		except:
			return 0
	
	def GetFileInfo(self, fname):
		"""
			Given a filename, if the file is stored in DFS
			return its fid and fsize. INTERNAL USE ONLY.
			DOES NOT HAVE TO BE ACCESSED FROM THE MDS
		"""
		query = """select fid, fsize from inode where fname="%s" """ % fname
		try:
			self.c.execute(query)
			result = self.c.fetchone()
			return result[0], result[1]
		except:
			return None, None

	def GetFiles(self):
		"""Returns the attributes of the files stored in the DFS (fname and fsize)"""
		query = """select fname, fsize from inode where 1""" ;
		self.c.execute(query)	
		return self.c.fetchall()

	def AddBlockToInode(self, fname, blocks):
		"""
			Once the inode was created with the file's attributes
			and the data chunk copied to a datanode, this function
			makes the inode point to its corresponding data blocks.
			It does so by receiving the filename and a list of tuples
			of the form (ip, port, cid)
		"""
		fid = self.GetFileInfo(fname)[0]
		if not fid:
			return None
		for ip, port, cid in blocks:
			nid = self.CheckNode(ip, port)
			if nid:
				query = """insert into block (nid, fid, cid) values (%s, %s, "%s")""" % (nid, fid, cid)
				self.c.execute(query)
			else:
				return 0 
		return 1

	def GetFileInode(self, fname):
		"""
			Knowing the file name this function return the whole inode (i.e. its attributes
			and the list of data blocks with all the information to access the blocks, like
			node name, address, port, and the chunk of the file).
		"""
		fid, fsize = self.GetFileInfo(fname)
		if not fid:
			return None, None
		query = """select address, port, cid from dnode, block where dnode.nid = block.nid and block.fid=%s""" % fid
		self.c.execute(query)
		return fsize, self.c.fetchall() 
		
