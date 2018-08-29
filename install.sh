#!/bin/bash

sudo apt-get update
sudo apt-get --yes install maven
# sudo apt-get install -y openjdk-8-jdk
# sudo update-alternatives --config java
# sudo update-alternatives --config javac

sudo apt-get install -y memcached
sudo apt-get install -y sysstat

sudo apt-get install -y mysql-server
sudo ufw allow mysql
systemctl start mysql

git clone https://github.com/scdblab/BG.git

# Perform the following steps manually to understand more about the benchmark.

# 1. create a mysql user.
# sudo mysql -u root
# CREATE USER 'user'@'localhost' IDENTIFIED BY '123456';
# GRANT ALL PRIVILEGES on *.* to 'user'@'localhost' IDENTIFIED BY '123456';
# FLUSH PRIVILEGES;
# sudo service mysql stop
# 1.1 Update mysql config file to make table names not case sensitive.
# add lower_case_table_names  = 1 to /etc/mysql/mysql.conf.d/mysqld.cnf below [mysqld]
# sudo service mysql restart

# 2. create a schema. 
# mysql -u user -p
# create schema bg;

# scp the BG.jar to your virtual machine.
# cp ~/BG.jar BG/
# cd BG/

# 3. create the schema.
# java -jar BG.jar onetime -schema -db MySQL.Basic1R1TMySQLClient -p db.user=user -p db.passwd=123456 -p insertimage=false -p db.url=jdbc:mysql://127.0.0.1:3306/bg -p db.driver=com.mysql.jdbc.Driver -p dbtype=mysql -p loadmode=actualload -p machineid=0 -p numclients=1 -p insertimage=false

# 4. load the database. 
# java -jar BG.jar onetime -loadindex -db MySQL.Basic1R1TMySQLClient -p usercount=1000 -p friendcountperuser=10 -p confperc=1 -p threadcount=10 -p db.user=user -p db.passwd=123456 -p insertimage=false -p imagesize=0 -p useroffset=0 -P workloads/populateDB_1000 -p db.url=jdbc:mysql://127.0.0.1:3306/bg -p db.driver=com.mysql.jdbc.Driver -p machineid=0 -p numclients=1

# 5. create a directory to store the log files. (for validating consistency)
# mkdir /tmp/logs

# 6. Run MySQL client
# java -jar BG.jar onetime -t -s -db MySQL.Basic1R1TMySQLClient -P workloads/99SymmetricWorkload -P workloads/populateDB_1000 -p memcached.hosts=127.0.0.1 -p logdir=/tmp/logs -p db.user=user -p validationapproach=interval -p exportfile=DistrStats -p insertimage=false -p warmupthreads=10 -p requestdistribution=dzipfian -p expectedlatency=0.1 -p thinktime=0 -p warmup=90000 -p zipfianmean=0.27 -p db.passwd=123456 -p numloadthreads=10 -p initapproach=deterministic -p db.url=jdbc:mysql://127.0.0.1:3306/bg -p db.driver=com.mysql.jdbc.Driver -p ratingmode=false -p resourcecountperuser=0 -p friendcountperuser=10 -p confperc=1 -p threadcount=10 -p lockreads=false -p enforcefriendship=true -p numclients=1 -p maxexecutiontime=60 -p usercount=1000 -p enablelogging=true -p useroffset=0 -p numsockets=10 -p machineid=0

# Run Mysql + memcached client
# java -jar BG.jar onetime -t -s -db MySQL.Basic1R1TMemcachedMySQLClient -P workloads/99SymmetricWorkload -P workloads/populateDB_1000 -p memcached.hosts=127.0.0.1 -p logdir=/tmp/logs -p db.user=user -p validationapproach=interval -p exportfile=DistrStats -p insertimage=false -p warmupthreads=10 -p requestdistribution=dzipfian -p expectedlatency=0.1 -p thinktime=0 -p warmup=90000 -p zipfianmean=0.27 -p db.passwd=123456 -p numloadthreads=10 -p initapproach=deterministic -p db.url=jdbc:mysql://127.0.0.1:3306/bg -p db.driver=com.mysql.jdbc.Driver -p ratingmode=false -p resourcecountperuser=0 -p friendcountperuser=10 -p confperc=1 -p threadcount=10 -p lockreads=false -p enforcefriendship=true -p numclients=1 -p maxexecutiontime=60 -p usercount=1000 -p enablelogging=true -p useroffset=0 -p numsockets=10 -p machineid=0
