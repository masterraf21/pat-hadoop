hdfs dfs -rm -r -f /user/ubuntu/temp/*
hdfs dfs -rm -r -f /user/ubuntu/output

hadoop jar pat-hadoop.jar /user/ubuntu/tc3.txt /user/root/temp /user/root/output

hdfs dfs -cat /user/ubuntu/output/*