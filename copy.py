# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: copy.py (comes with 7 other files)

# Copy client for the DFS



import os
import sys
import socket
from Packet import *
from math import ceil

SUBCHUNK_BUFFER = 1024	# must be the same as in data-node.py
DNODE_BUFFER = 4096		# buffer should be big just in case data node list is big
CHUNKLIST_BUFFER = 8192	# must be the same as in meta-data.py; should be big just in case data node list is big



def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)



def copyToDFS(address, from_path, to_path):
	"""
		Copy file from local machine in 'from_path' to the remote DFS
		in 'to_path' by dividing it to blocks and sending them to the
		available data nodes (managed by MDS)
	"""

	# Create a connection to the data server
	print "Connecting to MDS..."
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(address)
	print "Connected!"


	# Create `put` packet with the server path and fsize, and send it to MDS
	fsize = os.path.getsize(from_path)
	p1 = Packet()
	p1.BuildPutPacket(to_path, fsize)
	sock.send(p1.getEncodedPacket())
	print "Sent `put` request to MDS for local file '%s'!" % from_path


	# Get the list of data nodes (if no error)
	msg = sock.recv(DNODE_BUFFER)
	sock.close()

	if msg == "DUP":
		print "Tried inserting a file that already exists! Exiting..."
		sys.exit(0)

	p2 = Packet()
	p2.DecodePacket(msg)
	nodelist = p2.getDataNodes()
	print "Received list of %d data nodes! Closing connection to MDS..." % len(nodelist)


	# Divide the file in blocks and send them to data nodes
	chunk, total = 1, 0
	cuota = int(ceil(float(fsize) / len(nodelist)))
	block_list = []

	fd = open(from_path, "rb")

	for ip, prt in nodelist:


		# Connect to data node and send data block `put` request
		sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock2.connect((ip, prt))
		p3 = Packet()
		p3.BuildCommand("put")
		sock2.send(p3.getEncodedPacket())
		print "\n\t- Sent `put` request to data node at %s:%s!" % (ip, prt)


		# Blocking dummy variable so that messages don't corrupt themselves
		OK = sock2.recv(2) # 2 because it's only "OK"
		if OK != "OK":
			print "\nReply from %s:%s is corrupted! Exiting..." % (ip, prt)
			sock2.close()
			fd.close()
			sys.exit(0)


		# Send cuota little by little
		count = 0

		while count < cuota and total < fsize:

			data = fd.read(SUBCHUNK_BUFFER)
			sent = sock2.send(data)
			count += sent
			total += sent

			# Blocking dummy variable so that messages don't corrupt themselves
			MORE = sock2.recv(4) # 4 because it's only "MORE"
			if MORE != "MORE":
				print "\nReply from %s:%s is corrupted! Exiting..." % (ip, prt)
				sock2.close()
				fd.close()
				sys.exit(0)


		# Notify data node that chunk was sent
		print "\t- Sent chunk #%d!" % chunk
		chunk += 1
		sock2.send("DONE")


		# Receive block id from data node
		bid = sock2.recv(36) # 36 because uuids are 36 in length
		block_list.append((ip, prt, bid))
		print "\t- Received '%s'. Saving for later..." % bid


		# Disconnect from data node
		sock2.close()
		print "\t- Disconnecting from data node..."


	# Close local file
	fd.close()


	# Check if whole file was sent
	if total == fsize:
		print "\nWhole file sent!!!"
	else:
		print "The file wasn't sent completely! Exiting..."
		sys.exit(0)


	# Notify the MDS where the blocks are saved
	sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock3.connect(address)
	print "Reconnecting to MDS..."
	p4 = Packet()
	p4.BuildDataBlockPacket(to_path, block_list)
	sock3.send(p4.getEncodedPacket())
	print "Sent block list! Disconnecting from MDS..."


	# Disconnect from MDS
	sock3.close()
	print "Done!"
	
	

def copyFromDFS(address, from_path, to_path):
	"""
		Contact the metadata server to ask for the file blocks,
		and then get the data blocks from the data nodes.
		Saves the data in 'to_path'.
	"""


	# Contact the MDS to ask for information of 'to_path'
	print "Connecting to MDS..."
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(address)
	print "Connected!"


	# Create `get` packet with the remote file name, and send it to MDS
	p1 = Packet()
	p1.BuildGetPacket(from_path)
	sock.send(p1.getEncodedPacket())
	print "Sent `get` request to MDS for remote file '%s'!" % from_path


	# If there is no error, retrieve the data blocks
	msg = sock.recv(CHUNKLIST_BUFFER)
	sock.close()

	if msg == "NFOUND":
		print "File '%s' doesn't exist in DFS server! Exiting..." % from_path
		sys.exit(0)

	p2 = Packet()
	p2.DecodePacket(msg)
	metalist = p2.getDataNodes()
	fsize = p2.getFileSize()
	print "Received list of %d chunks! Closing connection to MDS..." % len(metalist)


	# Save the file in local machine (in 'to_path')
	chunk, total = 1, 0
	fd = open(to_path, "wb")

	for ip, prt, cid in metalist:


		# Connect to data node and send data block `get` request
		sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock2.connect((ip, prt))
		p3 = Packet()
		p3.BuildGetDataBlockPacket(cid)
		sock2.send(p3.getEncodedPacket())
		print "\n\t- Sent `get` request to data node at %s:%s!" % (ip, prt)


		# Receive data little by little
		while True:

			data = sock2.recv(SUBCHUNK_BUFFER)
			if data == "DONE":
				break
			fd.write(data)
			total += len(data)

			# Blocking dummy variable so that messages don't corrupt themselves in data node
			sock2.send("MORE")


		# Disconnect from data node
		print "\t- Received chunk #%d!" % chunk
		chunk += 1
		sock2.close()
		print "\t- Disconnecting from data node..."


	# Close local file
	fd.close()


	# Check if file was recieved completely
	if total == fsize:
		print "\nWhole file received!!!"
	else:
		print "File wasn't received completely!"



def main():

	if len(sys.argv) != 3:
		usage()


	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")


	if len(file_from) == 3:

		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]

		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path '%s' is a directory. Please name a file." % to_path
			usage()

		elif os.path.isfile(to_path):
			print "Error: file '%s' already exists. Please name differently." % to_path
			usage()

		# Copy from DFS to local machine
		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) == 3:

		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]

		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path '%s' is a directory." % from_path
			usage()

		elif not os.path.isfile(from_path):
			print "Error: file '%s' doesn't exist." % from_path
			usage()

		# Copy from local machine to DFS
		copyToDFS((ip, port), from_path, to_path)

	else:
		usage()



if __name__ == "__main__":
	main()