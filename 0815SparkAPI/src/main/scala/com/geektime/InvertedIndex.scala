package com.geektime
//import org.apache.spark.sql.SparkSession

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.io.Source._

object InvertedIndex {

  private var inputFilePath: File = new File(".")
  private var outputDirPath: String = ""

  private val NPARAMS = 2

  private def printUsage(): Unit = {
    val usage =
      """Spark RDD API InvertedIndex Test
        |Usage: localInputFile localOutputFile
        |localInputFile - (string) local file to read in test
        |localOutputFile - (string) Ourput directory for write tests""".stripMargin

    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    inputFilePath = new File(args(i))
/*    if (!inputFilePath.exists) {
      System.err.println(s"Given path (${args(i)}) does not exist")
      printUsage()
      System.exit(1)
    }*/

/*    if (!inputFilePath.isFile) {
      System.err.println(s"Given path (${args(i)}) is not a file")
      printUsage()
      System.exit(1)
    }*/

    i += 1
    outputDirPath = args(i)
    val outputDir = new File(args(i))
    // 如果目标文件目录存在， 则先删除后插入,若目录不为空需要先删除目中文件
    if (outputDir.exists) {
//      println(getFiles(outputDir).foreach(str => println(str)))
      getFiles(outputDir).foreach(file => file.delete())
      outputDir.delete()
    }
  }

  //获取指定单个目录下所有文件
  def getFiles(dir: File): Array[File] = {
      dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles)
  }

// IDEA 中参数 ： input output
// textFile 读取指定文件 ：$ProjectFileDir$\src\main\resources\words.txt $ProjectFileDir$\src\main\resources\result
// textFile 读取指定文件夹下所有文件，可以包括子目录或者指定正则表达式 ：$ProjectFileDir$\src\main\resources\words.txt $ProjectFileDir$\src\main\resources\result
  def main(args: Array[String]): Unit = {
    // 参数接收 解析
    parseArgs(args)

   val sparkConf: SparkConf = new SparkConf()
   sparkConf.setAppName("SparkRDDInvertedIndex") // 设置应用名称,该名称在Spark Web Ui中显示
   sparkConf.setMaster("local[1]") // 设置本地模式

   // 创建SparkContext
   val sc: SparkContext = new SparkContext(sparkConf)
   // 读取数据
   val file: RDD[String] = sc.textFile(inputFilePath.toString)
   // 切分并压平
   val words: RDD[String] = file.flatMap(_.split(" ")).flatMap(_.split("\t"))
   // 组装
   val wordAndOne  = words.map((_, 1))
   // 分组聚合
   val result  = wordAndOne.reduceByKey(_ + _)
   // 排序 降序
   // 第一个参数 - , 第二个参数 _2 , 降序
   val finalRes: RDD[(String, Int)] = result.sortBy(_._2, false)
   // 直接存储到local disk中
   finalRes.repartition(1).saveAsTextFile(outputDirPath)
   // 释放资源
   sc.stop()
  }
}
