package org.freemind.spark.basic

import com.hadoop.compression.lzo.LzoCodec
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * First Created by dev on 1/25/16.
*/
/*
VERY IMPORTANT NOTES
 To get Lzo to work
 sudo apt-get install lzop
 lzop file (compress)
 lzop -d < compressed-file > un-compress file name, rea *
 Then follow instruction in
 http://www.cloudera.com/content/www/en-us/documentation/cdh/5-0-x/CDH5-Installation-Guide/cdh5ig_cdh5_install.html#concept_5dp_jph_gk_unique_1
 https://github.com/twitter/hadoop-lzo

 The following is useful when you store lzo compressed files in HDHS
 need to add /etc/apt/sources.list.d/gplextras.list with
 deb [arch=amd64] http://archive.cloudera.com/gplextras5/ubuntu/precise/amd64/gplextras precise-gplextras5 contrib
 deb-src http://archive.cloudera.com/gplextras5/ubuntu/precise/amd64/gplextras precise-gplextras5 contrib
  to install hadoop-lzo

  sudo apt-get install hadoop-lzo
  dpkg -L hadoop-lzo to see where files being installed
$ ls -l /usr/lib/hadoop/lib/hadoop-lzo*
-rw-r--r-- 1 root root 62358 Dec  2 10:54 hadoop-lzo-0.4.15-cdh5.5.1.jar
lrwxrwxrwx 1 root root    30 Dec  2 10:54 hadoop-lzo.jar -> hadoop-lzo-0.4.15-cdh5.5.1.jar

need to modify core-site.xml
core-site.xml
<property>
<name>io.compression.codecs</name>
<value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec</value>
</property>
<property>
<name>io.compression.codec.lzo.class</name>
<value>com.hadoop.compression.lzo.LzoCodec</value>
</property>

to index it in-process via:
hadoop jar /path/to/your/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer big_file.lzo

index it in a map-reduce job via:
hadoop jar /path/to/your/hadoop-lzo.jar com.hadoop.compression.lzo.DistributedLzoIndexer big_file.lzo

 Either way, after 10-20 seconds there will be a file named big_file.lzo.index. The newly-created index file tells the LzoTextInputFormat's getSplits function how to
 break the LZO file into splits that can be decompressed and processed in parallel. Alternatively, if you specify a directory instead of a filename,
 both indexers will recursively walk the directory structure looking for .lzo files, indexing any that do not already have corresponding .lzo.index files.()

 bin/spark-shell --jars /usr/lib/hadoop/lib/hadoop-lzo.jar --driver-library-path /usr/lib/hadoop/lib/native --driver-class-path /usr/lib/hadoop/lib
 or bin/spark-submit make import LzoTextInputFormat work, add those jars to classpath
 */
/**
  * However, If you store your data in s3 instead HDFS, do the following
check the following
http://spark.apache.org/docs/latest/configuration.html#compression-and-serialization
http://spark.apache.org/docs/latest/configuration.html#compression-and-serialization

  Check book "Spark: Big Data Cluster Computing in Production" for general guideline
  */
/**
  * Created by dev on 12/14/16.
  */
object FileLzoSaveLoad {

  def main(args: Array[String]) {
    if (args.length < 2)
      throw new IllegalArgumentException

    val sc = new SparkContext(new SparkConf())
    val lzoInputDir = args(0)
    val lzoTextOutputDir = args(1)

    //Can I read textFile directly
    val lzoInput = sc.newAPIHadoopFile(lzoInputDir, classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text]).map(_._2.toString)
    lzoInput.take(10).foreach(println)

    lzoInput.saveAsTextFile(lzoTextOutputDir, classOf[LzoCodec])

  }
}
