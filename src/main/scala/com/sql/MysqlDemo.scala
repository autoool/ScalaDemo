package com.sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object MysqlDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession :SparkSession=SparkSession.builder().appName("test")
      .master("[local[2]").getOrCreate()

    import sparkSession.implicits._
    val urlData :DataFrame=sparkSession.read.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://localhost:3306/url_count",
        "driver"->"com.mysql.jdbc.Driver",
        "dbtable"->"url_data",
        "user"->"root",
        "password"->""
      )).load()

//    urlData.printSchema()
//    urlData.show()

//    val fData :Dataset[Row] = urlData.filter(x=>{
//      x.getAs[Int](0)>2
//    })
//    val rs:DataFrame = fData.select($"xuyuan",$"number_one")

    val  r :Dataset[Row] = urlData.filter($"uid">2)
    val rs :DataFrame = r.select($"xuyuan",$"number_one")
    rs.show()
    sparkSession.stop()
  }
}
