package org.freemind.spark.basic

import org.apache.hadoop.io.{BytesWritable, IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by dev on 12/14/16.
  */
object FileSequenceSaveLoad {

  def main(args: Array[String]) {
    if (args.length < 1)
      throw new IllegalArgumentException

    if (args.length < 1)
      throw new IllegalArgumentException
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sequenceDir = args(0)
    val inputSeq = "/user/hive/warehouse/countries_seq"

    //In order for  sequence file being saved as gz format, native library is required.
    //Text and IntWritable is not Serializable.  Therefore, it they need to convert into Serializable by using map
    val rdd = sc.parallelize(Seq(("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)), 1)
    rdd.saveAsSequenceFile(sequenceDir)
    //It created Hadoop output folder with part-00000 and _SUCCESS
    //hadoop fs -cat to inspect the Key class and value class
    val rdd_seq = sc.sequenceFile(sequenceDir, classOf[Text], classOf[IntWritable])
    rdd_seq.foreach(println)

    //Apparent the sequence file saved by Spark is different from the sequence file generated by Hive with STORED AS SEQUENCEFILE
    //countries_seq is created with STORED AS SEQUENCEFILE using the default field separator \1,
    //hadoop fs -cat to know the key class and value class,(header is at the first line
    //SEQ"org.apache.hadoop.io.BytesWritableorg.apache.hadoop.io.Textp�X`|��w�x��d7�
    val seqInput = sc.sequenceFile(inputSeq, classOf[BytesWritable], classOf[Text]).map{ case (x,y) => y.toString.split("\1")}.map(x => (x{0}, x(1)))
    seqInput.foreach(println)

  }



}
