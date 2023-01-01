### Setup spark (run with HDFS file)

#### 1. Download package
```shell
download https://spark.apache.org/downloads.html
#untar and rename to spark
#move file to /usr/local/spark
#set permission and log in with hadoopuser
#config ~/.bashrc
nano ~/.bashrc
export SPARK_HOME="/user/local/spark"
export PATH=$PATH:$SPARK_HOME/bin
```

#### 2. Config spark
```shell
#SPARK_MASTER
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh

nano spark-env.sh
#content
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
SPARK_YARN_QUEUE="default"

#SPARK_SLAVE
cd $SPARK_HOME/conf
cp slaves.template slaves
vi slaves
slave1
slave2
```

#### 3. Repeat installation in slave (through scp)

#### 4. Run spark
```shell
#run hadoop file system first
#run spark cluster
cd $SPARK_HOME/sbin
./stop-all.sh
./start-all.sh
```

#### 5. Submit
```shell
$ spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 1G \
–conf spark.yarn.submit.waitAppCompletion=false \
wordcount.py

#run with example
$ cd $SPARK_HOME
$ bin/run-example SparkPi 10
```
