cluster:
    python mr.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data
local:
    python mr.py u.data


start HBase
  sudo sudo /usr/hdp/current/hbase-master/bin/hbase-daemon.sh start rest -p 8000 --infoport 8001

start Mysql
    mysql -uroot -proot
    
start install cassandra
-------------repos-----------
[datastax]
name = Datastax repo
baseurl=http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0
-----------------------------
sudo yum install dsc30
sudo pip install cqlsh


    
create keyspace igormovies with replication ={'class':'SimpleStrategy', 'replication_factor':'1'} and durable_writes =true;
cqlsh> use igormovies;
cqlsh:igormovies> create table igortablemovies(user_id int, age int, gender text, occupation text, zip text, primary key(user_id))



mysql import to hadoop
sqoop import --connect 'jdbc:mysql://localhost/movielens?user=root&password=root' --driver com.mysql.jdbc.Driver --table movies -m 1

    
