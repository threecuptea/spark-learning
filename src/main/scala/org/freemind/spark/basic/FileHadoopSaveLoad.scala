package org.freemind.spark.basic

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
* First Created by dev on 1/18/16.
*/
/**
  * Created by dev on 12/14/16
  * This is only useful when you want to read those files written by MapReduce job.  Those files would be in KeyValue pair format
  * If you manually add those files by using hadoop put.  You should be able to read those files using textFile.
  * Spark would automatically convert Hadoop Text to java String.  The result is a  MapPartitionsRDD with lines of String or
  * more accurately PairRDDFunction
  */

object FileHadoopSaveLoad {

  def main(args: Array[String]) {

    //I have to use KeyValueTextInputFormat.  The default separator of KeyValueTextInputFormat is '\t'
    //countries_raw is created with FIELDS TERMINATED BY '\t'
    if (args.length < 2)
      throw new IllegalArgumentException
    val sc = new SparkContext(new SparkConf())
    val hadoopDir = args(0) //outputDir for hadoop files writen using \t as a separator
    val hadoopDir2 = args(1) //outputDir for hadoop files written using \1 as a separator

    val inputRaw = "/user/hive/warehouse/countries_raw"
    val inputRaw2 = "/user/hive/warehouse/countries_raw2"

    //newAPIHadoopFile works with FileInputFormat like files in HDFS but does not work HBASE.  We have to use newAPIHadoopRDD in that case
    val hadoopInput = sc.newAPIHadoopFile(inputRaw, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text])
      .map{case (x, y) => (x.toString, y.toString)}
    hadoopInput.foreach(println)

    //countries_raw2 is created using Hive default field separator.  That's \1
    val hadoopConf = new Configuration()
    hadoopConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\1")
    val hadoopInput2 = sc.newAPIHadoopFile(inputRaw2, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text], hadoopConf)
    hadoopInput2.foreach(println)

    //It will be saved as \t
    val rdd3 = sc.parallelize(Seq(("panda",0), ("pink",3), ("pirate",3), ("panda",1), ("pink",4)), 1).map{ case (x,y) => (new Text(x), new IntWritable(y))}.cache()

    //Save NewAPIHadoopFile using \t, we got to use PairRDDFunction RDD
    rdd3.saveAsNewAPIHadoopFile(hadoopDir, classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text,IntWritable]])

    val hadoopInput3 = sc.newAPIHadoopFile(hadoopDir, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text])
      .map{case (x, y) => (x.toString, y.toString.toInt)}
    hadoopInput3.foreach(println)

    //Save NewAPIHadoopFile using \1
    rdd3.saveAsNewAPIHadoopFile(hadoopDir2, classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text,IntWritable]], hadoopConf)

    val hadoopInput4 = sc.newAPIHadoopFile(hadoopDir2, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text], hadoopConf)
      .map{case (x, y) => (x.toString, y.toString.toInt)}
    hadoopInput4.foreach(println)


    sc.stop()
  }

}
