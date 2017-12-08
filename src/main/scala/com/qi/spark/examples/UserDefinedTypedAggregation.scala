package com.qi.spark.examples

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object UserDefinedTypedAggregation {

  case class Employee(name: String, salary: Double)

  case class Average(var salarySum: Double, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {

    def zero = Average(0d, 0l)

    override def reduce(b: Average, a: Employee): Average = {
      b.salarySum += a.salary
      b.count += 1
      return b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.salarySum += b2.salarySum
      b1.count += b2.count
      return b1
    }

    override def finish(reduction: Average): Double = {
      return reduction.salarySum / reduction.count
    }

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL qi-defined Datasets aggregation example")
      .getOrCreate()
    import spark.implicits._
    val ds:Dataset[Employee] = spark.read.json("src/main/resources/employees.json").as[Employee]
    ds.show()
    val averageSalary: TypedColumn[Employee, Double] = MyAverage.toColumn.name("average_salary)")
    val result: Dataset[Double] = ds.select(averageSalary)
    result.show()
    spark.stop()
  }
}
