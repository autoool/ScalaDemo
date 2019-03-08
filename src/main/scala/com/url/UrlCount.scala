package com.url

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 统计访问量排名前三
  */
object UrlCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("E:\\Personal\\BigData\\data-source\\itstar.log")
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val s: Array[String] = line.split("\t")
      (s(1), 1)
    })
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val rdd4: Array[(String, Int)] = rdd3.sortBy(_._2, false).take(3)
    rdd4.foreach(x => {
      println("url: " + x._1 + " 访问量：" + x._2)
    })

    println(rdd4.toBuffer)
  }

}
