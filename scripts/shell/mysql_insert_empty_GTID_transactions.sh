# !/bin/bash

echo "Enter username: "
read -s USER
echo "Enter password: "
read -s PWD
echo "Enter master host: "
read MASTER
echo "Enter slave host: "
read SLAVE

MASTER_GTID=$(mysql -h$MASTER -u$USER -p$PWD -ss -e"SHOW GLOBAL VARIABLES LIKE 'gtid_executed'" 2>&1 | grep -v "Warning:" | awk '{print $2}')
SLAVE_GTID=$(mysql -h$SLAVE -u$USER -p$PWD -ss -e"SHOW GLOBAL VARIABLES LIKE 'gtid_executed'" 2>&1 | grep -v "Warning:" | awk '{print $2}')

DIFF_GTID=$(mysql -h$SLAVE -u$USER -p$PWD -ss -e"SELECT GTID_SUBTRACT('$MASTER_GTID', '$SLAVE_GTID')" 2>&1 | grep -v "Warning:" );
DIFF_GTID=${DIFF_GTID//\\n/}

if [ "$DIFF_GTID" != " " ]
then
        echo "GTID positions that need to added on the slave are: "$DIFF_GTID

        IFS=',' read -ra GTID_ARRAY <<< "$DIFF_GTID"
		
        for GTID in "${GTID_ARRAY[@]}"
        do
			ID=$(echo $GTID | cut -d':' -f1)
			SEQ_START=$(echo $GTID | cut -d':' -f2 | cut -d'-' -f1)
			SEQ_END=$(echo $GTID | cut -d':' -f2 | cut -d'-' -f2)
			
			while [ "$SEQ_START" -le "$SEQ_END" ] 
			do
				mysql -h$SLAVE -u$USER -p$PWD -e"STOP SLAVE;SET GTID_NEXT = '$ID:$SEQ_START';BEGIN;COMMIT;SET GTID_NEXT=AUTOMATIC;" 2>&1 | grep -v "Warning:"

				((SEQ_START++))
			done						
        done
				
		mysql -h$SLAVE -u$USER -p$PWD -e"SHOW SLAVE STATUS\G" 2>&1 | grep -v "Warning:"
		
		echo "Missing GTIDs have been added to slave. You can now enable slaving on "$SLAVE		
else
        echo "GTID set on master and slave are compatible to enable replication."
        exit 0
fi;

