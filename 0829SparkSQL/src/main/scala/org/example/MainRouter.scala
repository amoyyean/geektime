package org.example

import org.apache.spark.sql.SparkSession

object MainRouter {
  def main(args: Array[String]): Unit = {
//    System.setProperty("","")
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(new MyCustomSparkExtension)
      .getOrCreate()

    // 设置Log Console输出量
    val ssc = spark.sparkContext
    ssc.setLogLevel("ERROR")

//    val df = spark.read.json("C:\\Users\\49921\\Desktop\\GeekTime\\geektime\\0829SparkSQL\\src\\main\\scala\\org\\example\\people.json")
//    df.toDF().write.saveAsTable("person")
    println(System.getenv("SPARK_HOME"))
    spark.sql("SHOW VERSION").show
    spark.stop()

  }
}
