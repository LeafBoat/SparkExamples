package com.qi.spark.examples.mllib

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession


object CorrelationExample {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("correlation example")
      .getOrCreate()
    import sparkSession.implicits._
    val vector: linalg.Vector = Vectors.sparse(4, Seq((0, 1.0), (3, -2.0)))
    val array = vector.toArray
    print(array.toString)
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    val tuples: Seq[Tuple1[linalg.Vector]] = data.map(Tuple1.apply)
    val dataFrame = tuples.toDF("features")
  }
}
