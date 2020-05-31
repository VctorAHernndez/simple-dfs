# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: data-node.py (comes with 7 other files)

# Data node server for the DFS



import os
import sys
import uuid
import socket
import SocketServer
from Packet import *

DATA_PATH = ''			# global on purpose (initial value doesn't matter)
SUBCHUNK_BUFFER = 1024	# must be the same as in copy.py



def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)



def register(meta_ip, meta_port, data_ip, data_port):
	"""
		Creates a connection with the metadata server and
		registers as data node
	"""

	# Establish connection
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((meta_ip, meta_port))

	# Attempt registration
	try:
		sp = Packet()
		sp.BuildRegPacket(data_ip, data_port)
		sock.send(sp.getEncodedPacket())
		response = sock.recv(3) # 3 because it's only "ACK"/"NAK"/"DUP"

		if response == "DUP":
			print "Already registered to MDS! Resuming activity..."
		elif response == "NAK":
			print "Registration Error!"
			sock.close()
			sys.exit(0)
	finally:
		sock.close()



class DataNodeTCPHandler(SocketServer.BaseRequestHandler):


	def handle_put(self):
		"""
			Receives a block of data from a copy client, and 
			saves it with an unique ID. The ID is sent back to the
			copy client.
		"""


		# Blocking dummy variable so that messages don't corrupt themselves in copy.py
		self.request.send("OK")


		# Generates a unique block id
		blockid = str(uuid.uuid1())


		# Open the file for the new data block
		name = os.path.join(DATA_PATH, blockid + ".dat")
		fd = open(name, "wb")
		print "\t- Creating chunk '%s'..." % (blockid + ".dat")


		# Receive the data block and write to disk
		while True:
			data = self.request.recv(SUBCHUNK_BUFFER)
			if data == "DONE":
				break
			fd.write(data)

			# Blocking dummy variable so that messages don't corrupt themselves in copy.py
			self.request.send("MORE")


		# Close chunk file
		fd.close()


		# Send the block id (36-character string) back
		self.request.send(blockid)
		print "\t- Sent chunk id to copy.py!"


	def handle_get(self, p):
		"""Sends a block of data to a copy client."""


		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data and send it back to the copy client
		name = os.path.join(DATA_PATH, blockid + ".dat")
		fd = open(name, "rb")
		print "\t- Reading chunk '%s'..." % name


		# Send the chunk little by little
		while True:

			data = fd.read(SUBCHUNK_BUFFER)
			if not data:
				break
			self.request.send(data)

			# Blocking dummy variable so that messages don't corrupt themselves
			MORE = self.request.recv(4) # 4 because it's only "MORE"
			if MORE != "MORE":
				print "\nReply from copy.py is corrupted! Exiting..."
				fd.close()
				sys.exit(0)


		# Notify copy.py that chunk was sent
		self.request.send("DONE")
		fd.close()
		print "\t- Sent chunk to copy.py!"


	def handle(self):
		msg = self.request.recv(1024) # 1024 will suffice (ips, ports, uuids aren't big)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()

		if cmd == "put":
			print "\nHandling `put` request from copy.py..."
			self.handle_put()
		elif cmd == "get":
			print "\nHandling `get` request from copy.py..."
			self.handle_get(p)
		else:
			print "\nNo `cmd` was specified..."



def main():

	META_PORT = 8000

	if len(sys.argv) < 4:
		usage()
	
	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		global DATA_PATH
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
	except:
		usage()

	print "Starting server at '%s' in port %s..." % (HOST, PORT)
	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

	try:
		server.serve_forever()
	except KeyboardInterrupt:
		print "\nClosing Data Node Server..."
	finally:
		server.server_close()
		print "Succesfully closed!"



if __name__ == "__main__":
	main()