while read host
do
        echo "connect to" $host
	ssh $host rmdir /usr/local/hadoop/hadoop_data/hdfs/datanode < /dev/null
	ssh $host mkdir /usr/local/hadoop/hadoop_data/hdfs/datanode < /dev/null
done < ${HADOOP_CONF_DIR}/slaves
