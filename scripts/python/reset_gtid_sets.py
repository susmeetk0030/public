#!/usr/bin/python3

import sys
import subprocess
import getpass
import mysql.connector

Master_UUID = ""
Slave_UUID 	= ""
Master_gtid_exec = ""
Slave_gtid_exec = ""
slave_diff = ""


def run_query(host, qry):
	mydb = mysql.connector.connect(host=host, user=DB_user, password=DB_pass, ssl_disabled='True', charset='utf8', use_unicode=True)
	mycursor = mydb.cursor()
	mycursor.execute(qry)

def run_fetch_query(host, qry):
	mydb = mysql.connector.connect(host=host, user=DB_user, password=DB_pass, ssl_disabled='True', charset='utf8', use_unicode=True)
	mycursor = mydb.cursor()
	mycursor.execute(qry)
	result = mycursor.fetchall()

	if result == []:
		val = ""
	else:
		val = result
	return val

def get_server_uuid(host):
	query = "SELECT @@SERVER_UUID"
	val = run_fetch_query(host, query)
	return val[0][0]		

def get_exec_gtid(host):
	query = "SELECT @@GLOBAL.gtid_executed"
	val = run_fetch_query(host, query)
	return val[0][0]
	
def get_gtid_diff(host):
	query = "SELECT GTID_SUBTRACT(\""+ Slave_gtid_exec +"\", \"" + Master_gtid_exec + "\")"
	val = run_fetch_query(host, query)
	return val[0][0]
	
def stop_slave(host):
	print("Running STOP SLAVE on " + Slave + "...")
	query = "STOP SLAVE"
	run_query(host, query)
	
def reset_master(host):
	print("Running RESET MASTER on " + Slave + "...")
	query = "RESET MASTER"
	run_query(host, query)
	
def set_gtid_purged(host, gtid_exec, diff):
	print("Purging GTID set on " + Slave + "...")
	for df in diff.split(","):
		gtid_purged = gtid_exec.replace(df, "")
		gtid_purged = gtid_purged.replace(",,", ",")
		gtid_exec = gtid_purged
	query = "SET GLOBAL GTID_PURGED = \"" + gtid_purged[:-1] + "\";"
	run_query(host, query)	

def display_info():
	global Master_UUID 
	global Slave_UUID 	
	global Master_gtid_exec 
	global Slave_gtid_exec 
	global slave_diff 

	Master_UUID = get_server_uuid(Master)
	Slave_UUID 	= get_server_uuid(Slave)
	Master_gtid_exec = get_exec_gtid(Master).replace("\n", "")
	Slave_gtid_exec = get_exec_gtid(Slave).replace("\n", "")
	slave_diff = get_gtid_diff(Slave)
		
	print("-----------------------------------------------------------------------------")
	print("Master UUID\t\t| " + Master_UUID)
	print("Slave UUID\t\t| " + Slave_UUID)
	print("Master GTID_EXECUTED\t| " + Master_gtid_exec)
	print("Slave GTID_EXECUTED\t| " + Slave_gtid_exec)
	print("GTID diff between Master & Slave| " + slave_diff)
	print("-----------------------------------------------------------------------------")

def get_master_host(host):
	query = "SHOW SLAVE STATUS;"
	result = run_fetch_query(host, query)
	if result == "":
		val = ""
	else:	
		val = result[0][1]
	return val
	
def has_slaves(host):
	query = "SHOW SLAVE HOSTS;"
	result = run_fetch_query(host, query)
	if result == "":
		val = 0
	else:	
		val = 1
	return val
	
if len(sys.argv) == 4:
	DB_user = sys.argv[1]
	DB_pass = sys.argv[2]
	Slave 	= sys.argv[3]
	Master 	= get_master_host(Slave)

else:
	DB_user = input("Enter Slave DB Username: ")
	DB_pass = getpass.getpass("Enter DB Password: ")
	Slave 	= input("Enter Slave Hostname: ")
	Master 	= get_master_host(Slave)

if Master == "":
	print("Host "+ Slave + " is not a Slave")
	exit(0)

display_info()

if slave_diff == "":
	print("No GTID difference found")
	print("Exiting...")
	exit(0)

if has_slaves(Slave) == 1:
	print("Host "+ Slave + " has slave/s")
	print("Exiting...")
	exit(0)	

if len(sys.argv) != 4:
	response = input("Do you want to remove " + slave_diff + " from " + Slave + "?(y/N)")
else:
	response = "y"
	
if response == "y":
	stop_slave(Slave)
	Slave_gtid_exec = get_exec_gtid(Slave)
	slave_diff = get_gtid_diff(Slave)
	reset_master(Slave)
	set_gtid_purged(Slave, Slave_gtid_exec, slave_diff)
	
	display_info()
	
else:
	print("Exiting...")
