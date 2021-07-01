#!/usr/bin/env python

import MySQLdb, ConfigParser, sys, io

def get_config():
    global var_host, var_user, var_pwd, var_sock
    global list_remove_str, list_colname_to_check, list_exclude_cols

    config = ConfigParser.ConfigParser()
    f = open(sys.argv[1], 'rb')
    f.readline()
    config.readfp(f)

    var_host = config.get('MYSQL', 'host')
    var_user = config.get('MYSQL', 'user')
    var_pwd = config.get('MYSQL', 'password')
    var_sock = config.get('MYSQL', 'socket')

    list_remove_str = config.get('CONFIG', 'remove_string').split(', ')
    list_colname_to_check = config.get('CONFIG', 'columns_to_check').split(', ')

    list_exclude_cols = config.get('EXCLUDE_COLUMNS', 'exclude_cols_list').split(',')
    f.close()

def clean_actn_stmnt(stmnt, replace_list):
    for replace_char in replace_list:
        stmnt_tmp = stmnt.replace(replace_char, '')
        stmnt = stmnt_tmp
    return stmnt_tmp

def add_to_trigger_dict(tbl, stmnt):
    col_list = list()
    if stmnt.count('SET') == 1:
        final_stmnt_tmp = stmnt.split('SET')[1].replace('IF;', '').replace(';','')
        col = final_stmnt_tmp.split(' = ')[0].replace(" ", "")
        col_list.append(col)

    elif stmnt.count('SET') > 1:
        for stmnt_tmp in stmnt.split('IF;'):
            if stmnt_tmp.count('SET') > 0:
                stmnt_tmp = stmnt_tmp.split('SET')[1].replace(";", " AND ")
                final_stmnt_tmp = stmnt_tmp.split('AND')[0]
                col = final_stmnt_tmp.split(' = ')[0].replace(" ", "")
                col_list.append(col)

    trigger_dict[tbl] = col_list

def generate_col_check_query():
    global qry_cols_to_check
    qry_cols_to_check = ""
    for x in list_colname_to_check:
        qry_cols_to_check = qry_cols_to_check + "COLUMN_NAME LIKE '%"+ x +"%' OR "
    return qry_cols_to_check.rstrip(" OR ")

error_msg = ""
error_code = 0

trigger_dict = {}
other_dict = {}

tbl_name = ""

get_config()

conn = MySQLdb.connect(host = var_host, user = var_user, passwd = var_pwd, unix_socket = var_sock)
cur = conn.cursor()

cur.execute("SELECT DISTINCT EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE, ACTION_STATEMENT FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_MANIPULATION = 'INSERT'")
results = cur.fetchall()
for result in results:    
    trigger_body = clean_actn_stmnt(result[2], list_remove_str)
    add_to_trigger_dict(result[0] + "." + result[1], trigger_body)

cur.execute("SELECT CONCAT(TABLE_SCHEMA, '.',TABLE_NAME), COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE (" + generate_col_check_query() + ") AND TABLE_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')")
results = cur.fetchall()
for result in results:
    if tbl_name == "":
        tbl_name = result[0]
        col_list = list()
        col_list.append(result[1])
    elif tbl_name == result[0]:
        col_list.append(result[1])
    else:
        if len(col_list) > 0:
            other_dict[tbl_name] = col_list
        col_list = list()
        col_list.append(result[1])
        tbl_name = result[0]        

for x in other_dict:
    for y in trigger_dict:
        if x == y:
            for i in other_dict[x]:
                if i.lower() not in (j.lower() for j in trigger_dict[y]):
                    if x + "." + i not in list_exclude_cols:
                        error_msg = error_msg + x + "." + i + ", "
                        error_code = 1
cur.close()
conn.close()

print error_msg
print(sys.exit(error_code))
