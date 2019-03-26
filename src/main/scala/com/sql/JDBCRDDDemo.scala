package com.sql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCRDDDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val connectoin = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC",
        "root","")
    }
    val jdbcRdd: JdbcRDD[(Int, String, String)] = new JdbcRDD(
      sc,
      connectoin,
      "select * from url_data where uid>=? and uid <= ?",
      1, 4, 2,
      r => {
        val uid = r.getInt(1)
        val xueyuan = r.getString(2)
        val number_one = r.getString(3)
        (uid, xueyuan, number_one)
      }
    )
    val jrdd = jdbcRdd.collect()
    println(jrdd.toBuffer)
    sc.stop()
  }
}
