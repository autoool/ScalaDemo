package com.url

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable


/**
  * 自定义分区
  */
object UrlParCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    System.setProperty("hadoop.home.dir", "D:\\Program Files\\hadoop-2.7.5")
    val rdd1: RDD[String] = sc.textFile("E:\\Personal\\BigData\\data-source\\itstar.log")
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val s: Array[String] = line.split("\t")
      (s(1), 1)
    })
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val rdd4: RDD[(String, (String, Int))] = rdd3.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      val xHost: String = host.split("[.]")(0)
      (xHost, (url, t._2))
    })

    val xueyuan: Array[String] = rdd4.map(_._1).distinct().collect()
    var partiner: XueyuanPartitioner = new XueyuanPartitioner(xueyuan)
    val rdd5: RDD[(String, (String, Int))] =
      rdd4.partitionBy(partiner).mapPartitions(it => {
        it.toList.sortBy(_._2).reverse.take(1).iterator
      })
    rdd5.saveAsTextFile("E:\\parout")
    sc.stop()

  }

  class XueyuanPartitioner(xy: Array[String]) extends Partitioner {

    val rules: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
    var number = 0

    for (i <- xy) {
      rules += (i -> number)
      number += 1
    }

    override def numPartitions: Int = xy.length

    override def getPartition(key: Any): Int = {
      rules.getOrElse(key.toString, 0)
    }
  }

}
