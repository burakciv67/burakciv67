#!/bin/bash
#***************************************************************************************************************
# Description: FRAUD_KAFKA_LAST_DATA_CHECK_ALARM
#
# History:
# Date                          Name                    Description
# 2025-01-03                    Burak CIV               Created
#***************************************************************************************************************
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
ORIG_DIR=$SCRIPTPATH            # parents path
me=${0##*/}                     # my name
LOG_DIR=${ORIG_DIR}/Log
TMP_DIR=${ORIG_DIR}/Tmp
tmp_file=${TMP_DIR}/email.tmp
TSTAMP=$(date +"%Y%m%d")
log_file=${LOG_DIR}/${TSTAMP}.log
db_cnf_file=/Products/configs/application.properties
mail_sender=xxx@xxxx.com  ## opsa göre güncellenmelidir.
recipient=xxx@xxxx.com  ## opsa göre güncellenmelidir.
db_cnf_file=/u01_old/config/application.properties  # db şifrelerinin tutuldugu dosyanın pathi verilmelidir.
ALARM_TIME=15
KAFKA_SERVER="localhost:9092"
KAFKA_TOPICS=("XXXX" "YYYY" )

f_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S,%3N') | PID: $$ |$@" | tee -a $log_file
}
f_info() {
    f_log "INFO| $@";
}
f_error() {
    f_log "ERROR| $@";
}

read_db_config() {
    vpara_db_username=$(grep '^vpara.username=' $db_cnf_file | awk -F= '{print $2}')
    vpara_db_password=$(grep '^vpara.password=' $db_cnf_file | awk -F= '{print $2}')
    vpara_db_ip=$(grep '^vpara.ip=' $db_cnf_file | awk -F= '{print $2}')
    vpara_db_port=$(grep '^vpara.port=' $db_cnf_file | awk -F= '{print $2}')
    vpara_db_database=$(grep '^vpara.database=' $db_cnf_file | awk -F= '{print $2}')
}

send_email() {
    f_info "sending report mail."
    echo "Merhaba\n" > $tmp_file
    echo "Kafka topicde data sorunu vardır. Kontrol edilmelidir.\n" >> $tmp_file
    echo "Tesekkürler, " >> $tmp_file
    echo "Iyi calismalar." >> $tmp_file

    cat $tmp_file | mailx -s "KAFKA_TOPIC_CHECK_ALARMS $TSTAMP" -r burak.civ@vodafone.com burak.civ@vodafone.com
}

db_alarm(){

  local status_value="$1"  
  local alarm_name_value="$2"  

  sqlplus -s $vpara_db_username/$vpara_db_password@$vpara_db_ip:$bpps_db_port/$vpara_db_database <<!

SET PAGESIZE 0
SET HEADING OFF
SET ECHO OFF
set term off
set termout off
set feedback off
set show off
set verify off
set termout off
set serverout off
set serveroutput off
set trimspool on
alter session set NLS_DATE_FORMAT = 'yyyy-mm-dd HH24:mi:ss';

update vepas_alarm 
set status = '$status_value' ,OCCURENCEDATE = sysdate
where alarm_name = '$alarm_name_value';  

commit ;

!
}


for KAFKA_TOPIC in "${KAFKA_TOPICS[@]}"; do
    latest_offsets=$(sh /Products/kafka-3.4.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $KAFKA_SERVER --topic $KAFKA_TOPIC --time -1)
    while read -r line; do
	
        partition_offset=$(echo $line | awk -F: '{ print $3 }')
        partition_num=$(echo $line | awk -F: '{ print $1 ":" $2 }')
        # f_info "partition_offset: $partition_offset , partition_num: $partition_num "

        #Kafka'dan son mesajı al ve CreateTime'ı çıkar
        output=$(sh /Products/kafka-3.4.1/bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_SERVER --topic $KAFKA_TOPIC --partition $(echo $partition_num | awk -F: '{print $2}') --offset $((partition_offset - 1)) --max-messages 1 --property print.timestamp=true --property print.key=true --timeout-ms 1000 2>/dev/null)

        last_timestamp=$(echo "$output" | grep -oP 'CreateTime:\K\d+')
		
        if [ -z "$last_timestamp" ]; then
        f_info "$KAFKA_TOPIC içerisinde kayit bulunamadı.PROBLEM"
		db_alarm "ACTIVE" "$KAFKA_TOPIC"
         continue
			else
			#last_timestamp'ı date formatına çevir
			create_time=$(date -d @$(echo $last_timestamp | cut -c1-10) +"%Y-%m-%d %H:%M:%S")
			f_info "$KAFKA_TOPIC Son data zamanı :$create_time"
			
			# Zaman farkını hesapla
			current_time=$(date +%s%3N)
			time_difference_ms=$((current_time - last_timestamp))
			time_difference_sn=$((time_difference_ms / 1000))
			time_difference_min=$((time_difference_sn / 60))
			time_difference_hour=$((time_difference_min / 60))
#			f_info "$KAFKA_TOPIC Zaman farkı: $time_difference_ms ms  $time_difference_sn sn $time_difference_min min $time_difference_hour hour   "

				# 15 dakikadır kafkada data yoksa alarm üret. (opsdaki veri akısına göre süre güncellenebilir)
				if [ ${time_difference_min} -ge $ALARM_TIME ]; then
					f_info "$KAFKA_TOPIC PROBLEM. Alarm created for related topic."
				 	db_alarm "ACTIVE" "$KAFKA_TOPIC"
				    send_email
				else
					f_info "OK Kafka Queue Has Been Filling for topic $KAFKA_TOPIC"
				    db_alarm "PASSIVE" "$KAFKA_TOPIC"
				fi
        fi
    done <<< "$latest_offsets"
done
