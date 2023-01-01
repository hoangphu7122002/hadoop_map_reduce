### config and run hadoop

#### 1. Download necessary package
```shell
#Install Java
sudo apt install openjdk-8-jdk
#Install Hadoop package
sudo wget -P ~ https://mirrors.sonic.net/apache/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
```

#### 2. First setup hadoop
```shell
#unzip and rename
tar xzf hadoop-3.3.4.tar.gz
mv hadoop-3.3.4 hadoop

#add path for java8
nano ~/hadoop/etc/hadoop/hadoop-env.sh
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

#move to directory (for scp)
sudo mv hadoop /usr/local/hadoop

#add some environment variable
sudo nano /etc/environment
PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/local/hadoop/bin:/usr/local/hadoop/sbin"
JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre"
```

#### 3. Create hadoopuser
```shell
sudo adduser hadoopuser

#add permission
sudo usermod -aG hadoopuser hadoopuser
sudo chown hadoopuser:root -R /usr/local/hadoop/
sudo chmod g+rwx -R /usr/local/hadoop/
sudo adduser hadoopuser sudo (to add sudo root for this user)
```

#### 4. Setup network for master and slave (gen key-ssh, rename hostname and same hadoopuser) and login hadoopuser (su - hadoopuser)

#### 5. Config core-site.xml
```shell
sudo nano /usr/local/hadoop/etc/hadoop/core-site.xml
#content
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://master:9000</value>
</property>
</configuration>
```

#### 6. config hdfs-site.xml
```shell
sudo nano /usr/local/hadoop/etc/hadoop/hdfs-site.xml
#content
<configuration>
<property>
<name>dfs.namenode.name.dir</name>
<value>/usr/local/hadoop/data/nameNode</value>
</property><property>
<name>dfs.datanode.name.dir</name>
<value>/usr/local/hadoop/data/dataNode</value>
</property>
<property>
<name>dfs.replication</name>
<value>2</value>
</property>
</configuration>
```

#### 7. setup workers
```shell
sudo nano /usr/local/hadoop/etc/hadoop/workers #or slave
#content
slave1
slave2

#scp folder for slave
scp /usr/local/hadoop/etc/hadoop/* slave1:/usr/local/hadoop/etc/hadoop
scp /usr/local/hadoop/etc/hadoop/* slave2:/usr/local/hadoop/etc/hadoop
```

#### 8. run dfs
```shell
#format HDFS file system.
source /etc/environment hdfs namenode -format
hdfs namenode -format
start-dfs.sh

jps #check with terminal
master:9870 #check with url
```

#### 9. run yarn
```shell
#config yarn (in .bashrc or terminal)
export HADOOP_HOME="/usr/local/hadoop/"
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME

#confit yarn-site.xml (on slave)
sudo nano /usr/local/hadoop/etc/hadoop/yarn-site.xml

<property>
<name>yarn.resourcemanager.hostname</name>
<value>master</value>
</property>

#start
start-yarn.sh

#check: http://master:8088/cluster
```
