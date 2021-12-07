hdfs dfs -rm -f /user/root/temp/*
hdfs dfs -rm -f /user/root/output

hadoop jar pat-hadoop.jar /user/root/tc1.txt /user/root/temp /user/root/output

hdfs dfs -cat /user/root/output/*