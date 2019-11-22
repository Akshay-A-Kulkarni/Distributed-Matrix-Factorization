Spark - Matrix Factorization using ALS

Code authors
-----------
John Bica
Akshay Kulkarni
Geoff Korb
Shawn Martin

Installation
------------
1) The following components are installed:
- JDK 1.8 (OpenJDK 8)
- Scala 2.11.12
- Hadoop 2.9.2
- Spark 2.3.1 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
2) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.9.2
export SCALA_HOME=/usr/local/scala
export SPARK_HOME=/usr/local/spark
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PATH=$PATH:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin


3) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Initial
2) Open command prompt.
3) Navigate to directory where the project files unzipped.
4) Add or move the edges.csv file to the input folder of the project folder 
5) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input, job.name
	Other defaults acceptable for running standalone.
	For AWS customize the following to run AWS EMR Hadoop below:
	ams.emr.release, aws.bucket, aws.num.nodes, aws.instance.type
6) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
7) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
8) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
