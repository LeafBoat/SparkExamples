package com.qi.spark.examples.utils

import org.apache.spark.sql.SparkSession

object ReadDataSource {
  val sparkSession = SparkSession.builder().appName("read").getOrCreate()

  def readJson( dataSource: String): Unit = {
    val dataFrame = sparkSession.read.format("json").load(dataSource)
    dataFrame.show()
  }

  def readParquet(dataSource:String): Unit ={
    val dataFrame = sparkSession.read.format("parquet").load(dataSource)
    dataFrame.show()
  }
  def read(dataSource: String): Unit ={
    val dataFrame = sparkSession.read.load(dataSource)
    dataFrame.show()
  }
}
