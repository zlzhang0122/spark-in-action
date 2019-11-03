package com.github.spark

import org.apache.spark.sql.SparkSession

/**
  * @Author: zlzhang0122
  * @Date: 2019/11/3 5:46 PM
  */
object SimpleApp {
  def main(args: Array[String]): Unit ={
    val logFile = "/Users/zhangjiao/apps/spark-2.4.4-bin-hadoop2.6/README.md"
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate();
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
