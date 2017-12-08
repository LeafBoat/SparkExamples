package com.qi.spark.examples.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object UserDefinedUntypedAggregation {

  object MyAverage extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(
      StructField("salary", DoubleType, true) :: Nil
    )

    override def bufferSchema: StructType = StructType(StructField("salarySum", DoubleType) :: StructField("count", LongType) :: Nil)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0d
      buffer(1) = 0l
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      return buffer.getDouble(0) / buffer.getLong(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL qi-defined DataFrames aggregation example")
      .getOrCreate()

    spark.udf.register("myAverage", MyAverage)

    val dataFrame = spark.read.json("src/main/resources/employees.json")
    dataFrame.show()
    dataFrame.createTempView("employees")
    val averageSalaryDataFrame = spark.sql("select myAverage(salary) as averageSalary from employees")
    averageSalaryDataFrame.show()
    spark.stop()
  }
}
