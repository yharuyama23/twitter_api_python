#!/bin/sh
MYSQL_ID=
MYSQL_PWD=
MYSQL_HOST=
MYSQL_SCHEMA=
MYSQL_COMMAND_ARGS="-h ${MYSQL_HOST} -u${MYSQL_ID} -p\"${MYSQL_PWD}\" -D${MYSQL_SCHEMA}"
echo "========`date '+%F %T'` check LockData========"
#ロックデータの個数確認
SQL='"SELECT count(*) FROM t_crawling_lock;"'
echo ${SQL}
CMD="echo ${SQL} | mysql ${MYSQL_COMMAND_ARGS} -N"
LOCKDATA_COUNT=(`eval $CMD`)
echo "----"
echo ${LOCKDATA_COUNT}
if [ ${LOCKDATA_COUNT} -gt 0 ]; then
    BASE_TIME=`date -d '30 minutes ago' "+%F %T"`
    echo "-- 30 minutes ago=""${BASE_TIME}"
    SQL_DATE='"SELECT created_at FROM t_crawling_lock ORDER BY created_at DESC LIMIT 1;"'
    CMD4="echo ${SQL_DATE} | mysql ${MYSQL_COMMAND_ARGS} -N"
    DATE_RES=`eval $CMD4`
    echo "-- DATE_RES=""${DATE_RES}"
    BASE_TIME_SERIAL=`date -d "${BASE_TIME}" '+%s'`
    DATE_RES_SERIAL=`date -d "${DATE_RES}" '+%s'`
    echo "-- BASE_TIME_SERIAL=${BASE_TIME_SERIAL}"
    echo "-- DATE_RES_SERIAL=${DATE_RES_SERIAL}"
    #30分前に実行したままだったら強制終了
    if [ ${BASE_TIME_SERIAL} -ge ${DATE_RES_SERIAL} ]; then
        echo "!!!! forced termination !!!!"
        #pythonプロセス削除
        pgrep -f 'python' | xargs kill -9
        #chromeプロセス削除
        pgrep -f 'chrome' | xargs kill -9
        #キャッシュっぽいファイル削除
        find /tmp/ -name "pymp*" -exec rm -r {} \;
        find /tmp/ -name ".com.google.Chrome*" -exec rm -r {} \;
        #ロックデータ削除
        DEL_SQL='"DELETE FROM t_crawling_lock;"'
        CMD3="echo ${DEL_SQL} | mysql ${MYSQL_COMMAND_ARGS} "
        DEL_RES=(`eval $CMD3`)
    fi
fi
#ロックデータの個数確認
LOCKDATA_COUNT=(`eval $CMD`)
echo "----"
echo ${LOCKDATA_COUNT}
if [ ${LOCKDATA_COUNT} -eq 0 ]; then
    echo "create LockData"
    INS_SQL='"INSERT INTO t_crawling_lock VALUES(null,now());"'
    CMD2="echo ${INS_SQL} | mysql ${MYSQL_COMMAND_ARGS} "
    INS_RES=(`eval $CMD2`)
    ##echo $INS_RES
    #ちゃんとロックデータができてればpython実行
    LOCKDATA_COUNT=(`eval $CMD`)
    if [ ${LOCKDATA_COUNT} -eq 1 ]; then
        echo "crawling start"
        python /var/www/witone-python/crawling_api.py
        echo "crawling end"
        #python終了したらロックデータを消す
        DEL_SQL='"DELETE FROM t_crawling_lock;"'
        CMD3="echo ${DEL_SQL} | mysql ${MYSQL_COMMAND_ARGS} "
        DEL_RES=(`eval $CMD3`)
    else
         echo "!!! failed insert lock data !!!"
    fi
else
    echo "crawling task skipped. Because exist lock data!"
fi
echo "========`date '+%F %T'` end========"