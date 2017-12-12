package com.qi.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QuickExample {
  def main(args: Array[String]): Unit = {
    //local[2]表示创建两个本地线程
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
    // Split each line into words
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordMaps: DStream[(String, Int)] = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = wordMaps.reduceByKey(_ + _)

    wordCounts.count()

    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }
}
