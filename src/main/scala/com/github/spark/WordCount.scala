package com.github.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: zlzhang0122
  * @Date: 2019/9/15 下午6:03
  */
object WordCount{
  def main(args: Array[String]): Unit = {

    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sconf)

    val line: RDD[String] = sc.textFile("in")
    val words: RDD[String] = line.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    val result: Array[(String, Int)] = wordToSum.collect()

    result.foreach(println)
  }
}
