package com.qi.spark.examples.sql.hive

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.immutable

object SparkHiveExample {

  case class Record(key: Int, value: String)

  private val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  private val sparkSession: SparkSession = SparkSession.builder().appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    import sparkSession.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")
    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show(10)
    print("------------------------------------------------------------------")
    val orderDF = sql("SELECT key,value FROM src WHERE key < 10 ORDER BY key")

    val stringDS = orderDF.map {
      case Row(key: Int, value: String) => s"key:$key,value:$value"
    }
    stringDS.show()
    print("-------------------------------------------------------------------")
    val seqInt: immutable.Seq[Int] = 1 to 10
    val records: immutable.Seq[Record] = seqInt.map(i => Record(i, s"val_$i"))
    val recordsDF = sparkSession.createDataFrame(records)
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    sparkSession.stop()
  }
}
