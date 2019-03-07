package com

import org.apache.spark.SparkConf


object Test extends App {

  println("hello")
  val conf: SparkConf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")

}
