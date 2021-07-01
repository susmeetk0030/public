import sys
import subprocess


def fetch_details(CMD, host, user, pwd, master_gtid, slave_gtid):

    query = ""
    val = ""

    if CMD == "SERVER_UUID":
        query = "mysql -u" + user + " -p" + pwd + " -ss -e'SELECT @@SERVER_UUID'"
    elif CMD == "GTID_EXECUTED":
        query = "mysql -u" + user + " -p" + pwd + " -ss -e'SHOW GLOBAL VARIABLES LIKE \"gtid_executed\"'"
    elif CMD == "GTID_SUBSET":
        val = -1
        query = "mysql -u" + user + " -p" + pwd + " -ss -e'SELECT GTID_SUBSET(\"" + master_gtid + "\", \"" + slave_gtid + "\")'"
    elif CMD == "GTID_SUBTRACT":
        query = "mysql -u" + user + " -p" + pwd + " -ss -e'SELECT GTID_SUBTRACT(\"" + master_gtid + "\", \"" + slave_gtid + "\")'"

    if query != "":
        query = query.replace('\n','')
        ssh = subprocess.Popen(["ssh", host, query],shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()

        if result == []:
            err = ssh.stderr.readlines()
            print("Error querying " + host + " " + str(err))
        else:
            val = result[0].replace('\\n', '')

    return val.replace('\n', '')

if len(sys.argv) != 5:
    print("Pass arguments in the following order:  New_Master_Host DB_User DB_Pwd DB_hosts_list")
    exit(1)

DB_master = sys.argv[1]
DB_user = sys.argv[2]
DB_pass = sys.argv[3]
hosts_filepath = sys.argv[4]

master_uuid = fetch_details("SERVER_UUID", DB_master, DB_user, DB_pass, "", "")
master_gtid = fetch_details("GTID_EXECUTED", DB_master, DB_user, DB_pass, "", "").replace('gtid_executed\t', '')

with open(hosts_filepath) as f:
    slaves = f.read().splitlines()

for slave in slaves:
    gtid_subtract = "NULL"
    gtid_subset = "NULL"
    gtid_subset_result = ""
    slave_uuid = "NULL"
    slave_gtid = "NULL"

    if slave != DB_master:

        slave_uuid = fetch_details("SERVER_UUID", slave, DB_user, DB_pass, "", "")
        if slave_uuid <> "":
            slave_gtid = fetch_details("GTID_EXECUTED", slave, DB_user, DB_pass, "", "").replace('gtid_executed\t', '')
            if slave_gtid <> "":
                gtid_subset_result = fetch_details("GTID_SUBSET", slave, DB_user, DB_pass, master_gtid, slave_gtid)

        if gtid_subset_result <> "":
            if gtid_subset_result == "0":
                gtid_subset = "Slave GTIDs are NOT a SUBSET of Master GTIDs"
                gtid_subtract = fetch_details("GTID_SUBTRACT", slave, DB_user, DB_pass, master_gtid, slave_gtid)
            elif gtid_subset_result == "1":
                gtid_subset = "Slave GTIDs are a SUBSET of Master GTIDs"
                gtid_subtract = ""

    print("-----------------------------------------------------------------------------")
    print( "\033[1m" + "Slave\t\t|" + slave + " (UUID: " + slave_uuid + ")" + "\033[0m")
    print("Master\t\t|" + DB_master + " (UUID:" + master_uuid + ")")
    print("Slave GTID set\t|" + slave_gtid)
    print("Master GTID set\t|" + master_gtid)
    print("GTID subset\t|" + gtid_subset)
    print("GTID substract\t|" + gtid_subtract)

print("-----------------------------------------------------------------------------")
