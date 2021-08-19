package com.geektime

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable._

object InvertedIndex1 {

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
  // textFile 读取指定文件夹下所有文件，可以包括子目录或者指定正则表达式 ：$ProjectFileDir$\src\main\resources\input\* $ProjectFileDir$\src\main\resources\output
  def main(args: Array[String]): Unit = {
    // 参数接收 解析
    parseArgs(args)

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("SparkRDDInvertedIndex") // 设置应用名称,该名称在Spark Web Ui中显示
    sparkConf.setMaster("local[1]") // 设置本地模式

    // 创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    // 读取数据 , 可以根据数据文件的大小，预估min partition
    // wholeTextFiles允许你读取文件夹下所有的文件，比如多个小的文本文件， 返回[(k,v),(k1,v1),....]  k代表文件名 , v代表整个文件内容。
    val rawFile: RDD[(String, String)] = sc.wholeTextFiles(inputFilePath.toString, 2): RDD[(String, String)]
    // 将K文件全路径名， 提取出名字
    val validFileContent = rawFile.map(x => (x._1.split("/")(x._1.split("/").length - 1), x._2))
    // validFileContent为我们最后得到的（文件名，单词）对
    //    validFileContent.foreach(println)
    // 将文件文本内容中的换行符替换成空格，考虑到windows和linux平台换行符不同
    val words = validFileContent.map(content => (content._1, content._2.replaceAll("\r\n", " ")
      .replaceAll("\n", " ")
      .replaceAll(",", " ")
      .replaceAll("'", " ")
      .toLowerCase
    ))
    //    val words = validFileContent.map(content => (content._1, content._2))
    //    words.foreach(println)
    val result = words.flatMap(x => {
      // 切分并压平  , 组装 , 分组聚合 ， 写法1
      val wordAndOne = x._2.split("\\s+").flatMap(_.split("\t")).map((x._1, _, 1))
      wordAndOne
      //首先要根据行来切分  写法2
      /*val line = x._2.split("\n")
      //对每一行而言，需要根据单词拆分，然后把每个单词组成（文件名，单词）对，链到list上
      for (i <- line) {
        val word = i.split("\\s+").iterator
        while (word.hasNext) {
          val w = word.next()
          list += ((x._1, w))
        }
      }
      val list_end = list.drop(1)
      list_end*/
    }
    )
    val finalResult = result.map(x => ((x._2, x._1), 1)).reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2))).groupByKey().sortByKey()
      .map(x => {
        val item = x._2.iterator
        val list = ArrayBuffer[String]()
        while (item.hasNext) {
          val w = item.next().toString()
          list += w
        }
        // 按照文档ID 升序排列
        Tuple2(x._1, list.sorted)
      })
    //    finalResult.foreach(println)

    // 直接存储到local disk中
    finalResult.map(f => {
      var str = ""
      str += "\"" + f._1 + "\":  "
      var flag = true
      for (y <- f._2) {
        if (flag) {
          str += "{" + y
          flag = false
        } else {
          str += "," + y
        }
      }
      str += "}"
      str
    }).coalesce(1).saveAsTextFile(outputDirPath)
    // 释放资源
    sc.stop()
  }
}
