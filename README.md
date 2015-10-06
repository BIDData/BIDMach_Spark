# BIDMach_Spark
Code to allow running BIDMach on Spark including HDFS integration and lightweight sparse model updates (Kylix). 

<h3>Dependencies</h3>

This repo depends on BIDMat, and also on lz4 and hadoop. Assuming you've built a working BIDMat jar, copy these files into the lib directory of this repo. i.e.

<pre>cp BIDMat/BIDMat.jar BIDMach_Spark/lib
cp BIDMat/lib/lz4-*.*.jar BIDMach_Spark/lib</pre>

you'll also need the hadoop common library from your hadoop installation:

<pre>cp $HADOOP_HOME/share/hadoop/common/hadoop-common-*.*.jar BIDMach_Spark/lib</pre>

and then 

<pre>cd BIDMach_Spark
./sbt package</pre>

will build <code>BIDMatHDFS.jar</code>. Copy this back to the BIDMat lib directory:

<pre>cp BIDMatHDFS.jar ../BIDMat/lib</pre>

Make sure $HADOOP_HOME is set to the hadoop home directory (usually /use/local/hadoop), and make sure hdfs is running:
<pre>$HADOOP_HOME/sbin/start-dfs.sh</pre>
Then you should have HDFS access with BIDMat by invoking 
<pre>BIDMat/bidmath</pre>

<pre>saveFMat("hdfs://localhost:9000/filename.fmat")</pre> or
<pre>saveFMat("hdfs://filename.fmat")</pre>


