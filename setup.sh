../bin/hdfs dfs -rm /user/elizavet/output/*
../bin/hdfs dfs -rmdir /user/elizavet/output
javac -cp `../bin/hadoop classpath`:. InvIndexes.java
jar -cvf InvIndexes.jar *.class
../bin/hadoop jar InvIndexes.jar InvIndexes rfc output TCP UDP LAN PPP HDLC
../bin/hdfs dfs -cat /user/elizavet/output/part-00000
