package com.qi.spark.examples.sql

import com.qi.spark.examples.utils.ReadDataSource
import org.apache.spark.sql.{DataFrame, SparkSession}

object ManuallySpecifyingOptions {

  def genericLoadAndSaveFuction(spark: SparkSession): Unit = {
    val dataFrame = spark.read.load("src/main/resources/users.parquet")
    dataFrame.select("name", "favorite_color").write.save("output/name_favoriteColor.parquet")
  }

  def manuallySpecifyingOptions(sparkSession: SparkSession): Unit = {
    val dataFrame = sparkSession.read.format("json").load("src/main/resources/people.json")
    dataFrame.select("name", "age").write.format("parquet").save("output/name_age.parquet")
  }

  def parquetToJson(sparkSession: SparkSession): Unit = {
    val dataFrame = sparkSession.read.load("src/main/resources/users.parquet")
    dataFrame.write.format("json").save("output/users.json")
  }

  def directlyRunSqlOnFiles(sparkSession: SparkSession): Unit = {
    val dataFrame: DataFrame = sparkSession.sql("select * from parquet.`src/main/resources/users.parquet`")
    dataFrame.show()
    val dataFrame2 = sparkSession.sql("select * from json.`src/main/resources/employees.json`")
    dataFrame2.show()
  }

  def bucketBy(sparkSession: SparkSession): Unit = {
    val dataFrame = sparkSession.sql("select * from json.`src/main/resources/employees.json`")
    dataFrame.write.bucketBy(5, "name").sortBy("salary")
      .saveAsTable("employee_bucketed")
  }

  def partitionBy(sparkSession: SparkSession): Unit = {
    val usersDF = sparkSession.read.load("src/main/resources/users.parquet")
    usersDF.write
      .partitionBy("favorite_color")
      .format("parquet")
      .save("output/namesPartByColor.parquet")
  }

  def partitionAndBucket(sparkSession: SparkSession) = {
    val peopleDF = sparkSession.read.format("json").load("src/main/resources/people.json")
    peopleDF
      .write
      .partitionBy("favorite_fruit")
      .bucketBy(2, "age")
      .saveAsTable("people_patitioned_bucketd")
  }


  def runBasicParquetExample(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    //读取people.json以parquet格式保存到本地
    val peopleDF = sparkSession.read.json("src/main/resources/people.json")
    peopleDF.write.parquet("output/people.parquet")
    //读取people.parquet
    val parquetFileDF = sparkSession.read.parquet("output/people.parquet")
    //创建表
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val nameDF = sparkSession.sql("select name from parquetFile where age between 13 and 19")
    nameDF.map { attributes =>
      "name:" + attributes(0)
    }.show()
  }

  def runParquetSchemaMergingExample(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val squareDF = sparkSession.sparkContext.makeRDD[Int](1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squareDF.write.parquet("output/test_table/key=1")

    val cubeDF = sparkSession.sparkContext
      .makeRDD[Int](6 to 10)
      .map(i => (i, i * i * i)).toDF("value", "cube")
    cubeDF.write.parquet("output/test_table/key=2")

    val mergeDF = sparkSession.read.option("mergeSchema", "true").parquet("output/test_table")
    mergeDF.printSchema()
    mergeDF.show()
  }

  def runJsonDatasetExample(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    //SparkSession读取json格式文件
    val peopleDF = sparkSession.read.json("src/main/resources/people.json")
    peopleDF.printSchema()
    peopleDF.createOrReplaceTempView("people")
    val teengerNamesDF = sparkSession.sql("select name from people where age between 13 and 19")
    teengerNamesDF.show()
    //SparkSession创建Dataset
    val peopleDS = sparkSession.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""
        :: Nil
    )
    //SparkSession读取json格式Dataset
    val DSToDF = sparkSession.read.json(peopleDS)
    DSToDF.show()
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("load or save datasource")
      .getOrCreate()
    //    genericLoadAndSaveFuction(sparkSession)
    //    manuallySpecifyingOptions(sparkSession)
    //    parquetToJson(sparkSession)
    //    directlyRunSqlOnFiles(sparkSession)
    //    bucketBy(sparkSession)
    //        partitionBy(sparkSession)
    //    partitionAndBucket(sparkSession)
    //    runBasicParquetExample(sparkSession)
    //    runParquetSchemaMergingExample(sparkSession)
    //    runJsonDatasetExample(sparkSession)
    //        ReadDataSource.read("output/test_table")
  }
}
