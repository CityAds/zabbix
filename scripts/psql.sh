#!/bin/bash

# Written by demofly for Pg 9.4
# all you nedd to make it running - CREATE ROLE "${DBUSER}" LOGIN SUPERUSER PASSWORD "${DBPASS}";

ARG1="$1"
ARG2="$2"
ARG3="$3" # used in get_conflicts()
DBUSER="zabbix"
DBPASS="<SUPERDBPASSWORD>"
DBHOST=127.0.0.1

## MASTER MONITOR FUNCTIONS ##

get_databases() {
    NOTFIRST=""
    echo '{
    "data":['

    echo "SELECT datname FROM pg_stat_database_conflicts" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1 | while read DATABASE
    do
        test -z "${NOTFIRST}" || echo -n ','
        test -z "${NOTFIRST}" && NOTFIRST="yes"
        echo -n "
        { \"{#DATABASE}\":\"${DATABASE}\" }"
    done
    echo "
    ]
}
"
}

get_conflicts() {
    #ARG1 conflicts
    #ARG2 dbname
    #ARG3 confl_tablespace,confl_lock,confl_snapshot,confl_bufferpin,confl_deadlock (see SELECT * FROM pg_stat_database_conflicts;)
    echo "SELECT ${ARG3} FROM pg_stat_database_conflicts WHERE datname='${ARG2}'" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1
}

get_log_list() {
    NOTFIRST=""
    echo '{
    "data":['

    ls -1 /var/log/postgresql/*.log | while read PGLOGNAME
    do
        test -z "${NOTFIRST}" || echo -n ','
        test -z "${NOTFIRST}" && NOTFIRST="yes"
        echo -n "
        { \"{#PGLOGNAME}\":\"${PGLOGNAME}\" }"
    done
    echo "
    ]
}
"
}

get_bgwriter_stat() {
    #ARG1 bgwriter
    #ARG2 column
    echo "SELECT ${ARG2} FROM pg_stat_bgwriter" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1
}


## SLAVE MONITOR FUNCTIONS ##

get_slots() {
    NOTFIRST=""
    echo '{
    "data":['

    echo "SELECT slot_name FROM pg_replication_slots" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1 | while read SLOT
    do
        test -z "${NOTFIRST}" || echo -n ','
        test -z "${NOTFIRST}" && NOTFIRST="yes"
        echo -n "
        { \"{#SLOTNAME}\":\"${SLOT}\" }"
    done
    echo "
    ]
}
"
}

get_slaves() {
    NOTFIRST=""
    echo '{
    "data":['

    echo "SELECT client_addr FROM pg_stat_replication" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1 | while read SLAVEIP
    do
        test -z "${NOTFIRST}" || echo -n ','
        test -z "${NOTFIRST}" && NOTFIRST="yes"
        echo -n "
        { \"{#SLAVEIP}\":\"${SLAVEIP}\" }"
    done
    echo "
    ]
}
"
}

get_xlog_bytes_diff() {
    #ARG1 slave_diff
    #ARG2 SLAVEIP
    echo "SELECT pg_xlog_location_diff(pg_current_xlog_location(), replay_location) FROM pg_stat_replication WHERE client_addr='${ARG2}'" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1
}

is_slot_connected() {
    #ARG1 is_slave_online
    #ARG2 SLOTNAME
    RESULT=`echo "SELECT active FROM pg_replication_slots WHERE slot_name='${ARG2}';" | PGPASSWORD="${DBPASS}" psql -U "${DBUSER}" -h "${DBHOST}" postgres -A | tail -n+2 | head -n-1 -n1`
    if [[ `echo "${RESULT}" | wc -l` -eq 0 ]]
    then
        echo 0
        exit 0
    fi
    echo "${RESULT}" | while read SLOTSTATE
    do
        if [[ "${SLOTSTATE}" == "t" ]]
        then
            echo "1"
            exit 0
        else
            echo "0"
            exit 0
        fi
    done
}

get_workers_count_by_type () {
    ps aux | grep postgres | grep -P "\) ${ARG2} *\$" | wc -l
}

## MAIN ##

case "$1" in

discovery_databases)
    get_databases
    ;;
conflicts)
    get_conflicts
    ;;

bgwriter)
    get_bgwriter_stat
    ;;

discovery_slots)
    get_slots
    ;;
discovery_slaves)
    get_slaves
    ;;
discovery_pglogs)
    get_log_list
    ;;
slave_diff)
    get_xlog_bytes_diff
    ;;
is_slot_connected)
    is_slot_connected
    ;;
workers)
    get_workers_count_by_type
    ;;
esac
