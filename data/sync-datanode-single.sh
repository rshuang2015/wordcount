read -p "datanode hostname: " host
echo "connect to" $host
ssh $host rmdir /usr/local/hadoop/hadoop_data/hdfs/datanode < /dev/null
ssh $host mkdir /usr/local/hadoop/hadoop_data/hdfs/datanode < /dev/null
