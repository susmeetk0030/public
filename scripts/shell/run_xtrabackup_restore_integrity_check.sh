#!/bin/bash

if [ "$1" = "" ]
then
        RESTORE_DATE=$(date +%Y-%m-%d)
else
        RESTORE_DATE=$1
fi;

EMAIL_LIST="tdias@tyro.com,lyeung@tyro.com,skhaire@tyro.com,tjancar@tyro.com"

# Fetch replication details from Mysql 3306 master.info
REPL_MASTER_HOST="MASTER_HOST='$(head -4 /var/lib/mysql/master.info | tail -1)',"
REPL_MASTER_USER="MASTER_USER='$(head -5 /var/lib/mysql/master.info | tail -1)',"
REPL_MASTER_PASSWORD="MASTER_PASSWORD='$(head -6 /var/lib/mysql/master.info | tail -1)', MASTER_AUTO_POSITION=1;"
REPL_DETAILS=$REPL_MASTER_HOST$REPL_MASTER_USER$REPL_MASTER_PASSWORD

# Duration (in secs) for which replication needs to be monitored
REPL_CHECK_TIME=3600

# Interval (in secs) for which replication needs to be monitored
REPL_CHECK_INTERVAL=30
MYSQL_SOCKET='/var/run/mysql/mysql_3307.sock'

# Previous nights full nightly backup log file location is $LOG

# Integrity check restore log file needs to have same name as the backup file
RESTORELOG="/var/log/xtrabackup/xtrabackup_"$RESTORE_DATE"_restore_check.log"

# MySQL 3307 error log file
MYSQL_LOG='/var/log/mysql/mysql-restore-error.log'

# Get Mysql login credentials
eval $((egrep -A3 '\[client\]' ~/.my.cnf | tail -3) | sed "s/ = /=/")

# Remove the mysql 3307 cnf file after the process finishes
cleanup()
{
        rm -f /etc/restore_my.cnf
}

restore_lvsnapshot()
{
    if (echo $(hostname)|grep -q lgcy)
    then echo "$(date +%Y-%m-%d_%H:%M:%S) Skip LVM snapshot restore for lgcy since none taken after switching to /mnt/archive"
    else echo "$(date +%Y-%m-%d_%H:%M) Restoring snapshot"
         cd ~
         # Before snapshot merging
         umount  "$LV_DIR"
         if [ $? -ne 0 ]
         then
                 errorhandler_restore
         fi
         # Prevents false os_fs_growth alerts
         /usr/bin/systemctl restart nrpe

         # Merge
         /sbin/lvconvert --merge "$SNAPSHOT"
         if [ $? -ne 0 ]
         then
                 errorhandler_restore
         fi

         if ! mount |grep "$LV_DIR"
         then mount "$LV_DIR"
              if [ $? -ne 0 ]
              then errorhandler_restore
              fi
         fi

         echo "$(date +%Y-%m-%d_%H:%M) Restoring snapshot Complete!!!"
    fi
}

kill_mysql_3307()
{
        echo "$(date +%Y-%m-%d_%H:%M) Kill MySQL 3307 and other processes on /mnt/hdd"
        # Check if Mysqld with restore.cnf is running. If yes then kill
        if ps aux | grep mysqld | grep restore
        then
                ps aux | grep mysqld | grep restore | awk '{print $2}' | xargs kill
        else
                echo "$(date +%Y-%m-%d_%H:%M:%S) No process to kill"
        fi;

        cd ~

        # Kill all PIDs on /mnt/hdd | /mnt/archive
        while [ $(lsof | egrep '/mnt/hdd|/mnt/archive' | grep -v $$ | awk '{print $2}' | wc -l ) -gt 0 ]
        do
                lsof | egrep '/mnt/hdd|/mnt/archive' | grep -v $$ | awk '{print $2}' | xargs kill > /dev/null 2>&1
        done;

        echo "$(date +%Y-%m-%d_%H:%M:%S) Kill Complete!!!"

}

is_mysql_running()
{
        return_val=0

        # Check if Mysql is running
        MYSQL_3307_PROCESS=$(ps aux | grep mysqld | grep restore | awk '{print $2}')

        if [ -z "$MYSQL_3307_PROCESS" ]
        then
                return_val=1
        else
                count=0

                # Check in Mysql 3307 error log file if the instance is ready for connections
                while [ $(tail -10 $MYSQL_LOG | grep 'ready for connections.' | wc -l) -eq 0 ]
                do
                        ((count++))

                        # Retry 5 times
                        if [ $count -ge 5 ]
                        then
                                break
                        fi;
                        echo "$(date +%Y-%m-%d_%H:%M:%S) Waiting for MySQL service to start"
                        sleep 300
                done;

                # If mysql encountered some other issue while starting...
                if [ $count -ge 5 ]
                then
                        return_val=1
                else
                        mysql -ss -S "$MYSQL_SOCKET" -u"$user" -p"$password" -e"SELECT 1;"
                        if [ $? -ne 0 ]
                        then
                                return_val=1
                        fi;
                fi;

        fi;

        return $return_val
}

is_xtrabackup_running()
{
        echo "$(date +%Y-%m-%d_%H:%M:%S) Checking if Xtrabackup is running!!!"

        SLEEP_COUNT=0

        while [ $(ps -ef |grep run_xtrabackup | grep -v $$ | egrep -v 'grep|incremental' | wc -l) -gt 1 ]
        do if [ $SLEEP_COUNT -ge 9 ]
           then echo "$(date +%Y%m%d-%H%M) Been pausing more than 2 hour due to another process:"
                        ps -ef |grep xtrabackup
                        echo
                        echo "$(date +%Y%m%d-%H%M) exiting."
                        errorhandler_restore
           else echo "$(date +%Y%m%d-%H%M) Backups running...wait 15 mins"
                        SLEEP_COUNT+=1
                        sleep 900
           fi
        done
}

errorhandler_restore()
{
    mail -s "RESTORE FAILED: XTRABACKUP Test $(hostname) ${RESTORE_DATE}" "$EMAIL_LIST"
    exit 1
}

DBKAFKA_IP=$(gethostip -d dbslave-kafka)
MY_IP=$(/opt/puppetlabs/bin/facter ipaddress)
CLUSTER1_PHY=$(gethostip -d cluster1-db-3-mc)

# Only run the command if this machne is not dbslave-kafka
if [ "$DBKAFKA_IP" != "$MY_IP" ] && [ "$CLUSTER1_PHY" != "$MY_IP" ] 
then

  exec >$RESTORELOG 2>&1
  
  if grep -q 'completed OK!' $LOG
  then

      echo "########################################################################################################################"
      is_xtrabackup_running

      echo "$(date +%Y-%m-%d_%H:%M:%S) Proceeding to test the backup..."

      # Get backupdir location from backup log file
      BACKUP_DIR=$(grep 'Backup created in directory' $LOG | awk '{print $7}' | sed -e "s/'//g")
      if [ $? -ne 0 ]
      then
          errorhandler_restore
      fi;
      echo "$(date +%Y-%m-%d_%H:%M:%S) Backup directory located at $BACKUP_DIR"

      echo "$(date +%Y-%m-%d_%H:%M:%S) Checking Backup size"

      # This will also make sure that the Backup path exists
      BACKUP_SIZE=$(du -sk $BACKUP_DIR | head -1 | awk '{print $1}')
      if [ $BACKUP_SIZE -le $REASONABLE_FREE ]
      then
          errorhandler_restore
      fi;

      if (echo $(hostname)|grep -q lgcy)
      then echo "$(date +%Y-%m-%d_%H:%M:%S) Skip LVM snapshot for lgcy since there is no free extents after switching to /mnt/archive"
      else echo "$(date +%Y-%m-%d_%H:%M:%S) Taking snapshot..."
           # Create an LVM snapshot
           /sbin/lvcreate -s -L 150G -n hddbackupsnap $LV_HDD
           exit_code=$?
           if [ $exit_code -ne 0 ]
           then
               if [ $exit_code -ne 5 ]
               then
                   errorhandler_restore
               fi
           fi
           echo "$(date +%Y-%m-%d_%H:%M:%S) Snapshot complete!!!"
      fi

      echo "$(date +%Y-%m-%d_%H:%M:%S) Preparing backup..."
      /opt/xtrabackup/bin/xtrabackup --prepare --apply-log-only --target-dir=$BACKUP_DIR --use-memory=16G
      if [ $? -ne 0 ]
      then
          restore_lvsnapshot
          errorhandler_restore
      fi;
      echo "$(date +%Y-%m-%d_%H:%M:%S) Backup prepare complete!!!"

      echo "$(date +%Y-%m-%d_%H:%M:%S) Copying my.cnf"
      # Add '\' as escape character
      BACKUP_DIR_TMP=${BACKUP_DIR%/}
      if [ $? -ne 0 ]
      then
          restore_lvsnapshot
          errorhandler_restore
      fi;

      # Create the mysql 3307 cnf file from the template
      cp -p /opt/tyro/scripts/restore/restore_my.cnf_tmpl /etc/restore_my.cnf
      if [ $? -ne 0 ]
      then
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;

      # Replace the backup dir in the mysql 3307 cnf file
      sed -i "s:/var/lib/mysql:$BACKUP_DIR_TMP:g" /etc/restore_my.cnf
      if [ $? -ne 0 ]
      then
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;

      # Create the logs folder within the backup dir
      if [ ! -d "$BACKUP_DIR/logs" ]
      then
          mkdir $BACKUP_DIR/logs
      fi;

      echo "$(date +%Y-%m-%d_%H:%M:%S) Grant privileges to the backup taken"
      chown mysql.mysql -R "$BACKUPDIR"
      if [ $? -ne 0 ]
      then
                restore_lvsnapshot
                cleanup
                errorhandler_restore
      fi;

      chmod 760 -R "$BACKUPDIR"
      if [ $? -ne 0 ]
      then
                restore_lvsnapshot
                cleanup
                errorhandler_restore
      fi;

      echo "$(date +%Y-%m-%d_%H:%M:%S) Starting Mysql:3307 instance..."
      /usr/sbin/mysqld --defaults-file=/etc/restore_my.cnf --user=mysql &

      sleep 60

      is_mysql_running
      result=$?

      if [ $result -eq 0 ]
      then
          echo "$(date +%Y-%m-%d_%H:%M:%S) Mysql:3307 instance started"
      else
          kill_mysql_3307
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;

      echo "$(date +%Y-%m-%d_%H:%M:%S) Fetching slave info"

      sed -i "s/MASTER_AUTO_POSITION\=1\;/$REPL_DETAILS/g" $BACKUP_DIR/xtrabackup_slave_info
      if [ $? -ne 0 ]
      then
          kill_mysql_3307
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;

      echo "$(date +%Y-%m-%d_%H:%M:%S) Starting replication from $REPL_MASTER_HOST"
      echo "$(date +%Y-%m-%d_%H:%M:%S) mysql -S \"$MYSQL_SOCKET\"  -e \"STOP SLAVE; RESET MASTER; RESET SLAVE; source $BACKUP_DIR/xtrabackup_slave_info;\" -u\"$user\" -p"
      mysql -S "$MYSQL_SOCKET"  -e "STOP SLAVE; RESET MASTER; RESET SLAVE; source $BACKUP_DIR/xtrabackup_slave_info;" -u"$user" -p"$password"
      if [ $? -ne 0 ]
      then

          kill_mysql_3307
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;
      sleep 5

      mysql -S "$MYSQL_SOCKET"  -e "START SLAVE; SELECT SLEEP(10); SHOW SLAVE STATUS\G"  -u"$user" -p"$password"
      if [ $? -ne 0 ]
      then
          kill_mysql_3307
          restore_lvsnapshot
          cleanup
          errorhandler_restore
      fi;

      sleep 5

      echo "$(date +%Y-%m-%d_%H:%M:%S) Monitoring if replication is working..."
      TIME_CTR=0
      TIME_REMAINING=$REPL_CHECK_TIME

      while [ $TIME_CTR -le $REPL_CHECK_TIME ]
      do
          echo "$(date +%Y-%m-%d_%H:%M:%S) Continue Replication Monitor for $TIME_REMAINING secs..."

          SLAVE_IO_RUNNING=$(mysql -S "$MYSQL_SOCKET"  -e "SHOW SLAVE STATUS\G"  -u"$user" -p"$password" | grep 'Slave_IO_Running: ' | awk '{print $2}')
          SLAVE_SQL_RUNNING=$(mysql -S "$MYSQL_SOCKET"  -e "SHOW SLAVE STATUS\G" -u"$user" -p"$password" | grep 'Slave_SQL_Running: ' | awk '{print $2}')

          if [ "$SLAVE_IO_RUNNING" = "No" ] || [ "$SLAVE_SQL_RUNNING" = "No" ]
          then
              TIME_CTR=999999

              kill_mysql_3307
              restore_lvsnapshot
              cleanup
              errorhandler_restore
          else
              sleep $REPL_CHECK_INTERVAL
              TIME_REMAINING=$(expr $TIME_REMAINING - $REPL_CHECK_INTERVAL)
          fi;

          TIME_CTR=$(expr $TIME_CTR + $REPL_CHECK_INTERVAL)
      done;


      echo "$(date +%Y-%m-%d_%H:%M:%S) Backup tested. OK!!!"

      kill_mysql_3307
      restore_lvsnapshot
      cleanup

      echo "$(date +%Y-%m-%d_%H:%M:%S) Process Complete!!!"
      echo "Please check logs for more details." | mail -s "XTRABACKUP RESTORE COMPLETED SUCCESSFULLY" "$EMAIL_LIST"
  else
      echo "$(date +%Y-%m-%d_%H:%M:%S) Testing not required since the backup was not created successfully!!!"
      mail -s "XTRABACKUP RESTORE UNSUCCESSFUL" "$EMAIL_LIST" < $RESTORELOG
  fi
  exec >>$LOG 2>&1
  echo "$(date +%Y-%m-%d_%H:%M:%S) XTRABACKUP RESTORE process should have completed, see email for success or failure."
fi
