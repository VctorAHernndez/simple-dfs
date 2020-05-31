# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: mds_db.py (comes with 7 other files)

# List client for the DFS



import sys
import socket
from Packet import *

LS_BUFFER = 4096 # must be big if inserting many files into DFS



def usage():
	print """Usage: python %s <server>:<port, default=8000>""" % sys.argv[0] 
	sys.exit(0)



def client(ip, port):

	# Contacts the metadata server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))

	# Ask for list of files
	p = Packet()
	p.BuildListPacket()
	sock.send(p.getEncodedPacket())

	# Receive msg from MDS
	msg = sock.recv(LS_BUFFER)

	if msg == "NAK":
		print "Error ocurred when receiving `ls` response! Exiting..."
		sock.close()
		sys.exit(0)

	# Decode msg
	p.DecodePacket(msg)

	# Get list of (fname, fsize)
	flist = p.getFileArray()

	# Print fname followed by fsize
	for n, s in flist:
		print "%s %d bytes" % (n,s)

	# Close connection
	sock.close()



def main():

	if len(sys.argv) < 2:
		usage()

	ip, port = None, None
	server = sys.argv[1].split(":")

	if len(server) == 1:
		ip = server[0]
		port = 8000
	elif len(server) == 2:
		ip = server[0]
		port = int(server[1])

	if ip is None:
		usage()

	client(ip, port)



if __name__ == "__main__":
	main()