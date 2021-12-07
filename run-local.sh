hdfs dfs -rm -r -f /user/root/temp/*
hdfs dfs -rm -r -f /user/root/output

hadoop jar pat-hadoop.jar /user/root/tc3.txt /user/root/temp /user/root/output

hdfs dfs -cat /user/root/output/*