# -*- coding: utf-8 -*-

# Víctor A. Hernández & José R. Ortiz
# CCOM 4017 – Operating Systems
# Assignment 4: test.py (comes with 7 other files)

# Script to test the SQL support library for the DFS project



from mds_db import *


# mds_db()
db = mds_db("dfs.db") 


# Connect()
print "Connecting to database"
db.Connect()
print


# AddDataNode()
print "Testing node addition (two numbers must appear)"
print db.AddDataNode("136.145.54.10", 80)
print db.AddDataNode("136.145.54.11", 70)
print


# CheckNode()
print "Testing if node was inserted (a number must appear)"
print db.CheckNode("136.145.54.11", 70)
print


# GetDataNodes()
print "Testing all available data nodes (two lines must appear)"
for address, port in  db.GetDataNodes():
	print address, port
print 


# InsertFile()
print "Inserting two files to DB (two 1's must appear)"
print db.InsertFile("/hola/cheo.txt", 20)
print db.InsertFile("/opt/blah.txt", 30)
print


# GetFiles()
print "Files in the database (two lines must appear)"
for file, size in db.GetFiles():
	print file, size
print


# AddBlockToInode()
print "Adding blocks to database (duplicate message if not the first time running this script)"
try:
	db.AddBlockToInode("/hola/cheo.txt", [("136.145.54.11", 80, "1"), ("136.145.54.11", 70, "2")])
except:
	print "Won't duplicate"
print


# GetFileInode()
print "Testing retrieving inode info (from `/hola/cheo.txt`)"
fsize, chunks_info = db.GetFileInode("/hola/cheo.txt")
print "\t- File size is:", fsize
print "\t- File can be constructed from:"
i = 1
for address, port, chunk in chunks_info:
	print "\t\t%s. %s %s %s" % (i, address, port, chunk)
	i += 1
print


# Close()
print "Closing connection..."
db.Close() 
