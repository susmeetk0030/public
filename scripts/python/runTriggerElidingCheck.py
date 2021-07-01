#!/usr/bin/env python

import MySQLdb, ConfigParser, sys, io

def get_config():
    global var_host, var_user, var_pwd, var_sock, sample_row_size
    global ignore_db_list, ignore_tbl_list, list_spl_case1_db, list_spl_case1_tbl, list_remove_str

    config = ConfigParser.ConfigParser()
    f = open(sys.argv[1], 'rb')
    f.readline()
    config.readfp(f)

    var_host = config.get('MYSQL', 'host')
    var_user = config.get('MYSQL', 'user')
    var_pwd = config.get('MYSQL', 'password')
    var_sock = config.get('MYSQL', 'socket')

    ignore_db_list = config.get('CONFIG', 'ignore_db').split(',')
    ignore_tbl_list = config.get('CONFIG', 'ignore_tbl').split(',')    
    list_spl_case1_db = config.get('CONFIG', 'special_case1_db').split(',')
    list_spl_case1_tbl = config.get('CONFIG', 'special_case1_tbl').split(',')    
    list_remove_str = config.get('CONFIG', 'remove_string').split(',')
    sample_row_size = int(config.get('CONFIG', 'sample_row_size'))

def clean_actn_stmnt(stmnt, replace_list):
    for replace_char in replace_list:
        stmnt_tmp = stmnt.replace(replace_char, '')
        stmnt = stmnt_tmp
    return stmnt_tmp

def split_stmnt(stmnt):
    final_stmnt = ' '
    if stmnt.count('SET') == 1:
        final_stmnt_tmp = stmnt.split('SET')[1].replace('IF;', '').replace(';','')
        if 'random_id' not in final_stmnt_tmp and 'elide' not in final_stmnt_tmp and 'sysdate' not in final_stmnt_tmp and 'DATE_FORMAT' not in final_stmnt_tmp  and 'SUBSTR' not in final_stmnt_tmp and 'MD5' not in final_stmnt_tmp and 'id' not in final_stmnt_tmp and 'NULL' not in final_stmnt_tmp:
            final_stmnt = final_stmnt + " AND " + add_ifnull(final_stmnt_tmp)
    elif stmnt.count('SET') > 1:
        for stmnt_tmp in stmnt.split('IF;'):
            if stmnt_tmp.count('SET') > 0:
                stmnt_tmp = stmnt_tmp.split('SET')[1].replace(";", " AND ")
                final_stmnt_tmp = stmnt_tmp.split('AND')[0]
                if 'random_id' not in final_stmnt_tmp and 'elide' not in final_stmnt_tmp and 'sysdate' not in final_stmnt_tmp and 'DATE_FORMAT' not in final_stmnt_tmp  and 'SUBSTR' not in final_stmnt_tmp and 'MD5' not in final_stmnt_tmp and 'id' not in final_stmnt_tmp and 'NULL' not in final_stmnt_tmp:
                    final_stmnt = final_stmnt + " AND " + add_ifnull(final_stmnt_tmp)

    return final_stmnt.replace('\n','').rstrip(' AND')

def add_ifnull(stmnt):
    stmnt_tmp = stmnt.split(' = ')
    col = stmnt_tmp[0]
    val = stmnt_tmp[1]

    return "IFNULL(" + col.strip() + "," + val.strip() + ")" + " = " + val.strip()

def main():
    global var_host, var_user, var_pwd, var_sock, sample_row_size
    global ignore_db_list, ignore_tbl_list, list_spl_case1_db, list_spl_case1_tbl, list_remove_str

    error_code = 0
    error_msg = ""

    get_config()

    conn = MySQLdb.connect(host = var_host, user = var_user, passwd = var_pwd, unix_socket = var_sock)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE, ACTION_STATEMENT FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_MANIPULATION = 'INSERT'")
    results = cur.fetchall()
    for result in results:    
        trigger_body = clean_actn_stmnt(result[2], list_remove_str)
        where_clause = split_stmnt(trigger_body)
        if where_clause != "":

            if result[0] not in ignore_db_list and result[1] not in ignore_tbl_list:
                id_column_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + result[0] + "' AND TABLE_NAME = '" + result[1] + "'ORDER BY ORDINAL_POSITION LIMIT 1"
                cur.execute(id_column_query)
                id_column = cur.fetchone()[0]
                check_count_query = "SELECT MAX(" + id_column + ") FROM " + result[0] + "." + result[1]
                cur.execute(check_count_query)
                count_value = cur.fetchone()[0]
                if count_value >= sample_row_size:
                    if result[0] in list_spl_case1_db and result[1] in list_spl_case1_tbl:
                        inner_stmnt = "SELECT * FROM " + result[0] + "." + result[1] + " WHERE category = 'ERROR' ORDER BY 1 DESC LIMIT " + str(sample_row_size)
                    else:                 
                        inner_stmnt = "SELECT * FROM " + result[0] + "." + result[1] + " ORDER BY 1 DESC LIMIT " + str(sample_row_size)
                    statement = "SELECT COUNT(1) FROM (" + inner_stmnt + ")a WHERE 1=1 " + where_clause + ";"
                    cur.execute(statement)
                    for output in cur.fetchall():
                        if output[0] != sample_row_size:
                            error_msg = error_msg + result[0] + "." + result[1] + ", "
                            error_code = 1

    cur.close()
    conn.close()

    print error_msg
    print(sys.exit(error_code))

main()
