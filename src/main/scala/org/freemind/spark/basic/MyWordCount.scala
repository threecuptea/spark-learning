package org.freemind.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
First Created by dev on 12/30/15.

  To save job history, make log info (job, stage. storage and environment) stay beyond the job execution time (You can view it in Spark web UI localhost:4040),
  set --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=history-file-location> for application submitted.
  We can view job stage UI details in in ubuntu:8080 (default, 7080 if changed) assume you start Spark master/slave
   sbin/start-master.sh --webui-port 7080
   sbin/start-slave.sh --webui-port 7081 spark://ubuntu:7077
  No need to start history server in this case

 bin/spark-submit --master spark://ubuntu:7077 --name "MyWordCount" --class com.myspace.spark.basic.MyWordCount --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///var/log/spark \
 /home/dev/projects/samples/spark-tutorial/target/scala-2.10/spark-tutorial_2.10-1.0.0.jar /home/dev/Public/spark-1.6.3-bin-hadoop2.6/README.md output23

  bin/spark-submit --master yarn --name "MyWordCount" --class com.myspace.spark.basic.MyWordCount --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///var/log/spark \
 /home/dev/projects/samples/spark-tutorial/target/scala-2.10/spark-tutorial_2.10-1.0.0.jar /home/dev/Public/spark-1.6.3-bin-hadoop2.6/README.md output23

  You  start history server
  */
/**
  * Created by dev on 12/13/16.
  * Filter out punctuation and stopWords.  However, it fails to consolidate nouns of singular and plural
  */
object MyWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException()
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val outputDir = args(1)

    val stopWords = Array("","a","an","the","this","to","for","and","##","can","on","is","in","of","also","if","with","you","or")
    val punct = "\"'`,:.![]<>-"

    val words = input.flatMap(line => line.split(" "))
    val wc = words.map(w => w.dropWhile(punct.contains(_)).reverse.dropWhile(punct.contains(_)).reverse)
                  .filter(w => !stopWords.contains(w.toLowerCase))
                  .map(w => (w, 1))
                  .reduceByKey(_+_)

    wc.top(10)(Ordering[Int].on(_._2)).foreach(println) //action, job1
    wc.saveAsTextFile(outputDir) //always treat it as a directory with PART-00000 and _SUCCESS, action, job2
    //2 jobs, job 1 top, 2 stages: atage boundary is a transform that create ShuffledRDD: reduceByKey
    //2 actions produce 2 jobs
    //stage 0, textFile, flatMap, map, fliter, map
    //stage 1, reduceByKey, top, before shuffled, it swould save to disk or memory
    //job 2, will skip stage 2 and only execute stage 3 no matter cache or not.  Before shuffled, spark always save the result beforehand.
    //Theefore, it can completely skip stage 2.
    //# of tasks for each stage is dependent upon # partition.

    sc.stop()

  }

}
