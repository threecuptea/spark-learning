package org.freemind.spark.basic

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dev on 12/13/16.
  * Demostrate how to use accumulator properly
  */
object BasicLoadNums {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException()
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))
    val outputDir = args(1)

    val dataCnt = sc.accumulator(0)
    val errorCnt = sc.accumulator(0)

    //use happypandas, coffee 1,
    //Need to use flatMap instead of map because flatMap implements PartialFunction.  That would filter out None but Map won't
    val countByKey = input.flatMap{ line =>
      try {
        val parts = line.split(" ")
        val data = Some(parts(0), parts(1).toInt)
        dataCnt += 1
        data
      }
      catch {
        //pattern match
        case e@(_: NumberFormatException | _: IndexOutOfBoundsException) =>
          errorCnt += 1
          None
      }
    }.reduceByKey(_+_)
    //Stage 0 before reduceByKey.  It would do ShuffledWrite
    //stage 1 reduceByKey will shuffledRead

    countByKey.count() //dataCount or errort was incremented in transformation block which is lazy..
    //We need to use action to trigger execution
    if (errorCnt.value < dataCnt.value *.1) {
      countByKey.saveAsTextFile(outputDir+"_gz", classOf[GzipCodec])
    }
    else
      println(s"Too many error: ${errorCnt.value} for ${dataCnt.value}")

    sc.stop()
  }


}
