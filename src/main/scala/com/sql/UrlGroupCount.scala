package com.sql

import java.net.URL
import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

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
      val  conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
        "root", "")
      val sql = "insert into url_data (xueyuan,number_one) values(?,?)"
      val statement = conn.prepareStatement(sql)
      statement.setString(1, x._1)
      statement.setString(2, x._2.toString)
      statement.executeUpdate()
      statement.close()
      conn.close()
    })

    sc.stop()
  }
}
