# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: meta-data.py (comes with 7 other files)

# SQL support library. Database info for the metadata server



import sys
from mds_db import *
from Packet import *
import SocketServer

CHUNKLIST_BUFFER = 8192 # must be the same as in meta-data.py; should be big just in case data node list is big



def usage():
	print """Usage: python %s <port, default=8000>""" % sys.argv[0] 
	sys.exit(0)



class MetadataTCPHandler(SocketServer.BaseRequestHandler):


	def handle_reg(self, db, p):
		"""
			Register a new data node to the MDS.
				- ACK if successfully registered.
				- NAK if problem.
				- DUP if duplicate.
		"""

		print "\t- Adding data node to MDS..."

		try:
			if db.AddDataNode(p.getAddr(), p.getPort()):
				self.request.sendall("ACK")
				print "\t- Succesfully added!"
			else:
				self.request.sendall("DUP")
				print "\t- Data node already registered! Resuming activity..."
		except:
			self.request.sendall("NAK")
			print "\t- Couldn't add data node to MDS!"


	def handle_list(self, db):
		"""
			Get the file list from the database and send list to client
				- List of tuples if successfully retrieved.
				- NAK if problem.
		"""

		try:
			print "\t- Retrieving file list from DFS..."
			flist = db.GetFiles()
			p = Packet()
			p.BuildListResponse(flist)
			self.request.sendall(p.getEncodedPacket())
			print "\t- Sent file list to ls.py!"
		except:
			self.request.sendall("NAK")
			print "\t- Couldn't retrieve file list from DFS!"


	def handle_put(self, db, p):
		"""
			Insert new file in database and respond with list of available data nodes.
				- List of tuples if succesfully inserted
				- DUP if trying to insert a duplicate of the file.
		"""

		# Get (fname, fsize) from packet
		fname, fsize = p.getFileInfo()
		print "\t- Retrieving file info from packet..."
	
		# Insert file into MDS and send list of available data nodes back to copy.py
		if db.InsertFile(fname, fsize):
			print "\t- Inserted file into MDS! Sending back available data nodes..."
			nodelist = db.GetDataNodes()
			q = Packet()
			q.BuildPutResponse(nodelist)
			self.request.sendall(q.getEncodedPacket())
			print "\t- Done!"
		else:
			self.request.sendall("DUP")
			print "\t- File already exists in MDS!"
	

	def handle_get(self, db, p):
		"""
			Check if file is in database and return list of
			server nodes that contain the file.
				- List of fname and triple tuples
				- NFOUND if fname is not valid (file not present in DFS)
		"""


		# Get fname from packet
		fname = p.getFileName()
		print "\t- Retrieving file name from packet..."

		# Get fsize and metalist from MDS
		fsize, metalist = db.GetFileInode(fname)

		if fsize:
			print "\t- Sending `get` response to copy.py..."
			q = Packet()
			q.BuildGetResponse(metalist, fsize)
			self.request.sendall(q.getEncodedPacket())
			print "\t- Done!"
		else:
			self.request.sendall("NFOUND")
			print "\t- File not found in MDS!"


	def handle_blocks(self, db, p):
		"""Add the data blocks to the file inode"""

		# Fill code to get file name and blocks from packet
		print "\t- Retrieving file name and blocklist from packet..."
		fname = p.getFileName()
		blocklist = p.getDataBlocks()

		# Fill code to add blocks to file inode
		if db.AddBlockToInode(fname, blocklist):
			print "\t- Succesfully added blocks to MDS!"
		else:
			print "\t- An error ocurred when trying to add blocks to MDS..."

		
	def handle(self):

		# Establish a connection with the local database
		db = mds_db("dfs.db")
		db.Connect()

		# Define a packet object to decode packet messages
		p = Packet()

		# Receive a msg from the list, data-node, or copy clients
		msg = self.request.recv(CHUNKLIST_BUFFER)
		
		# Decode the packet received
		p.DecodePacket(msg)

		# Extract the command part of the received packet
		cmd = p.getCommand()

		# Invoke the proper action 
		if   cmd == "reg":
			# data-node.py polling for registration
			print "\nHandling `reg` request from %s:%s..." % (p.getAddr(), p.getPort())
			self.handle_reg(db, p)

		elif cmd == "list":
			# ls.py asking for a list of files
			print "\nHandling `ls` request..."
			self.handle_list(db)
		
		elif cmd == "put":
			# copy.py asking for servers to put data
			print "\nHandling `put` request..."
			self.handle_put(db, p)
		
		elif cmd == "get":
			# copy.py asking for servers to get data
			print "\nHandling `get` request..."
			self.handle_get(db, p)

		elif cmd == "dblks":
			# copy.py sending data blocks for file
			print "\nHandling `dblks` request..."
			self.handle_blocks(db, p)
		else:
			print "\nNo `cmd` was specified..."

		db.Close()



def main():

	HOST, PORT = "", 8000

	if len(sys.argv) > 1:
		try:
			PORT = int(sys.argv[1])
		except:
			usage()

	print "Starting server at 'localhost' in port %s..." % PORT
	server = SocketServer.TCPServer((HOST, PORT), MetadataTCPHandler)

	try:
		server.serve_forever()
	except KeyboardInterrupt:
		print "\nClosing Meta Data Server..."
	finally:
		server.server_close()
		print "Succesfully closed!"



if __name__ == "__main__":
	main()