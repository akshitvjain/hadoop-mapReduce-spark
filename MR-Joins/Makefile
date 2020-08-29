# Makefile for Hadoop MapReduce WordCount demo project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/Users/akshitjain/Desktop/Northeastern-University/Large-Scale/hadoop-2.9.2
jar.name=mr-joins-1.0.jar
jar.path=target/${jar.name}
job.name.path2=edu.neu.ccs.CountPath2
job.name.res=edu.neu.ccs.UserTriangleCounterRS
job.name.rep=edu.neu.ccs.UserTriangleCounterRep
local.input=input
local.output=output
local.step1.output=output_step1
# Pseudo-Cluster Execution
hdfs.user.name=akshit
hdfs.input=input
hdfs.output=output
hdfs.step1.output=output_step1
# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=akshit-mr-median
aws.subnet.id=subnet-6356553a
aws.input=input
aws.output=output
aws.step1.output=output_step1
aws.log.dir=log
aws.num.nodes=8
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.step1.output}* ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local-cp: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name.path2} ${local.input} ${local.output}

local-res: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name.res} ${local.input} ${local.step1.output} ${local.output}

local-rep: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name.rep} ${local.input} ${local.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.step1.output}* ${hdfs.output}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name.res} ${hdfs.input} ${hdfs.step1.output} ${hdfs.output}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name.res} ${hdfs.input} ${hdfs.step1.output} ${hdfs.output}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.step1.output}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

#"s3://${aws.bucket.name}/${aws.step1.output}",
# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Social Triangle RS" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name.res}","s3://${aws.bucket.name}/${aws.input}", "s3://${aws.bucket.name}/${aws.step1.output}", "s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f Jain_Akshit_HW2.tar.gz
	rm -rf build
	mkdir -p build/deliv/
	cp -r src build/deliv/Jain_Akshit_HW2
	#cp -r output build/deliv/Jain_Akshit_HW2
	cp syslog_rs_join_small.txt build/deliv/Jain_Akshit_HW2
	cp syslog_rs_join_large.txt build/deliv/Jain_Akshit_HW2
	cp syslog_rep_join.txt build/deliv/Jain_Akshit_HW2
	cp Akshit_Jain_HW2.pdf build/deliv/Jain_Akshit_HW2
	cp pom.xml build/deliv/Jain_Akshit_HW2
	cp Makefile build/deliv/Jain_Akshit_HW2
	cp README.txt build/deliv/Jain_Akshit_HW2
	tar -czf Jain_Akshit_HW2.tar.gz -C build/deliv Jain_Akshit_HW2
