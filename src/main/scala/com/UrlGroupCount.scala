package com

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：求出每个学院 访问第一位的网址
  * bigdata:video
  * java:video
  * python:teacher
  */

object UrlGroupCount {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile("E:\\Personal\\BigData\\data-source\\itstar.log")
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val s: Array[String] = line.split("\t")
      (s(1), 1)
    })
    //网址  总的访问量
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    //
    val rdd4: RDD[(String, String, Int)] = rdd3.map(x => {
      val url: String = x._1
      val host: String = new URL(url).getHost.split("[.]")(0)
      (host, url, x._2)
    })
    val rdd5: RDD[(String, List[(String, String, Int)])] = rdd4.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(1)
    })

    rdd5.foreach(x => {
      println("url: " + x._1 + " 访问量第一的：" + x._2)
    })

  }

}
