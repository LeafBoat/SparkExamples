package com.qi.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QuickExample {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.print("请添加产生数据源的主机及其端口")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("network worldcount")
    val scc = new StreamingContext(sparkConf, Seconds(2))
    val receiverInputDStream: ReceiverInputDStream[String] = scc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK)
    val dstream: DStream[String] = receiverInputDStream.flatMap(_.split(" "))
    val word_lable = dstream.map(k => (k, 1))
    val word_count: DStream[(String, Int)] = word_lable.reduceByKey((a, b) => a + b)
    word_count.print()
    scc.start()
    scc.awaitTermination()
  }
}
