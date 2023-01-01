### RUN CODE

#### 1. Run sample code
```shell
test.txt #data with multiple extension

#copy file to hadoop file
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -put test.txt /

#run code
yarn jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.2.jar wordcount “/" output

#get output
hdfs dfs -ls output
hdfs dfs -cat output/part-r-00000 | less
```

#### 2. Run your code
```shell
code .java file #example is WordCount.java

#compile
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class

#get input with same above step
hadoop jar wc.jar WordCount /user/wc/input /user/wc/output
```
