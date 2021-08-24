package com.geektime

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * 使用spark RDD API 实现 hadoop distcp 功能
 */
object SparkDistCP extends Serializable{

  // 定义源目录下所有目录集合
  private var sourceDirList = ArrayBuffer[String]()
  // 定义源目录下所有文件集合
  private var sourceFileList = ArrayBuffer[String]()
  // 定义（源文件名，目标文件名）二元组
  private var sourceTargetPathTuple = ListBuffer[(String, String)]()

  //配置信息类 , [*]有多少空闲cpu就开多少线程
  @transient  private val sparkConf = new SparkConf().setAppName("SprakDistcp").setMaster("local[*]")
  //上下文对象
  @transient  private val sparkContext = new SparkContext(sparkConf)

  @transient private val hadoopConf = sparkContext.hadoopConfiguration
  // bin/hdfs dfs -ls hdfs://192.168.2.100:8020/input
  private var hdfsInputPath = "hdfs://192.168.2.100:8020/input"
  private var hdfsOutputPath = "hdfs://192.168.2.100:8020/input_cp"
  @transient  private val fs = FileSystem.get(
    new java.net.URI(hdfsInputPath), hadoopConf)


  // RDD 并发度
  private var MAX_CONCURRENCE = 1

  // -i	IGNORE_FAILURES , FALSE : 遇到单个文件拷贝失败，job终止 , TRUE 单个文件失败，job不终止
  private var IGNORE_FAILURES = "FALSE"

  /**
   * 先将 inputDir 目录结构（包括子目录结构） 中所有目录及子目录遍历列出来
   *
   * @param inputDir source 目录
   * @param hdfs     FileSystem 类实现
   * @return
   */
  def traverseSourceDirsAndFiles(inputDir: Path, hdfs: FileSystem): Boolean = {
    //遍历inputDir目录
    val files = hdfs.listStatus(inputDir)
    files.foreach { fStatus => {
      if (fStatus.isDirectory) {
        //          println(fStatus)
        sourceDirList += fStatus.getPath.toString
        traverseSourceDirsAndFiles(new Path(fStatus.getPath.toString), hdfs)
      } else {
        sourceFileList += fStatus.getPath.toString
      }
    }
    }
    return true
  }


  /**
   * 分布式文件拷贝作业需要先将 inputDir 目录结构（包括子目录结构） 在target 目录下创建出来
   *
   * @param inputDir  source 目录
   * @param outputDir target 目录
   * @param hdfs      FileSystem 类实现
   * @return
   */
  def createDirs(inputDir: Path, outputDir: String, hdfs: FileSystem): Unit = {
    //遍历inputDir目录
    val files = sourceDirList
    files.foreach { fStatus => {
      // 将遍历出来的inputDir目录中所有文件夹， 前缀中inputDir部分替换成outputDir
      val targetDir = new Path(fStatus.replaceAll(inputDir.toString, outputDir))
      // 判断targetDir 是否存在， 不存在则创建 ， 存在跳过
      if (!hdfs.exists(targetDir)) {
        hdfs.mkdirs(targetDir)
      }
    }
    }
  }

  /**
   * 将源目录和目标目录 hdfs path 映射为元组对 (sourcePath , tartgetPath)
   *
   * @param inputDir  源输入目录
   * @param outputDir 目标目录
   */
  def generateSourceTargetPathTuple2(inputDir: Path, outputDir: String): Unit = {
    //    for (p <- sourceFileList) println(p)
    sourceFileList.foreach(singleFile => {
      sourceTargetPathTuple += ((singleFile.toString, singleFile.toString.replaceAll(inputDir.toString, outputDir)))
      sourceTargetPathTuple
    })
  }

  def distCpFile(): Unit = {
    val rdd = sparkContext.makeRDD(sourceTargetPathTuple, MAX_CONCURRENCE)
    val rdd1 = rdd.mapPartitions(value => {
      var result = ArrayBuffer[String]()
      while (value.hasNext) {
        val index = value.next()
        try {
          val flag = FileUtil.copy(fs, new Path(index._1), fs, new Path(index._2), false, hadoopConf)
          if (flag) {
            result += "From " + index._1 + " ---> " + index._2 + " Success !"
          }
        } catch {
          case ex: Exception => {
            if ("TRUE".equals(IGNORE_FAILURES)) {
              // 任务发生异常，跳过继续后续任务
              println(index._1 + " File Copy Failed !")
            } else {
              // 任务发生异常,立即终止，程序退出
              System.exit(1)
            }
          }
        }

      }
      result.iterator
    }).foreach(println)
  }

  // 定义输入参数个数
  private val NPARAMS = 2

  private def printUsage(): Unit = {
    val usage =
      """Spark RDD API DisyCP Test
        |Usage: sourceDir targetDir -i Ignore_failures -m max_concurrence
        |sourceDir - (string) local dir to cp in test
        |targetDir - (string) target directory for copy tests
        | -i 单个文件拷贝失败是否忽略，继续拷贝其它
        | -m 控制同时copy的最大并发task数""".stripMargin
    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {

    // hdfs://192.168.2.100:8020/input hdfs://192.168.2.100:8020/input_cp -i -m
    // 2// 两个必填参数 , 其余为可选参数
    val len = args.length
    if (len < NPARAMS) {
      printUsage()
      System.exit(1)
    }
    var argsMap: Map[String, String] = Map()
    var i = 0
    while (i < len) {
      if (0 == i) {
        // 默认第1个参数为SOURCE
        argsMap += ("SOURCE_PATH" -> args(i))
      } else if (1 == i) {
        // 默认第2个参数为TARGET
        argsMap += ("TARGET_PATH" -> args(i))
      } else {
        // 其余为可选参数 , 目前只支持-i 和 -m nums
        if ("-i".equals(args(i).toLowerCase)) {
          // 如果-i存在，开启设置为true
          argsMap += ("IGNORE_FAILURES" -> "TRUE")
        }
        if ("-m".equals(args(i).toLowerCase)) {
          // 如果-m存在，取后面紧跟内容为该参数value
          if (i > len - 1) {
            println("-m 后缺少参数值")
            System.exit(1)
          }
          try {
            var tempValue = args(i + 1).toInt
          } catch {
            case ex: NumberFormatException => {
              println("-m 参数值含有非法数字")
              System.exit(1)
            }
          }
          argsMap += ("MAX_CONCURRENCE" -> args(i + 1))
          i = i + 1
        }
      }
      i = i + 1
    }
    argsMap.keys.foreach(value => {
      println(value + " : " + argsMap(value))
    })
    if (argsMap.contains("SOURCE_PATH")) {
      hdfsInputPath = argsMap("SOURCE_PATH")
    }
    if (argsMap.contains("TARGET_PATH")) {
      hdfsOutputPath = argsMap("TARGET_PATH")
    }
    if (argsMap.contains("IGNORE_FAILURES")) {
      IGNORE_FAILURES = argsMap("IGNORE_FAILURES")
    }
    if (argsMap.contains("MAX_CONCURRENCE")) {
      MAX_CONCURRENCE = argsMap("MAX_CONCURRENCE").toInt
    }
  }

  def main(args: Array[String]): Unit = {
    // 参数接收 解析
    parseArgs(args)

    val input = new Path(hdfsInputPath)
    val hdfs = FileSystem.get(
      new java.net.URI(hdfsInputPath), hadoopConf)
    // 遍历源目录的所有目录及文件
    traverseSourceDirsAndFiles(input, hdfs)
    //    for (p <- sourceDirList) println(p)
    //    println("---------------------------------------------------------------------")
    //    for (p <- sourceFileList) println(p)

    // 在目标文件夹下创建不存在目录
    createDirs(input, hdfsOutputPath, hdfs)

    // (hdfs://192.168.2.100:8020/input/day/word.txt,hdfs://192.168.2.100:8020/input_cp/day/word.txt)
    // (hdfs://192.168.2.100:8020/input/one/word.txt,hdfs://192.168.2.100:8020/input_cp/one/word.txt)
    // (hdfs://192.168.2.100:8020/input/word.txt,hdfs://192.168.2.100:8020/input_cp/word.txt)
    generateSourceTargetPathTuple2(input, hdfsOutputPath)
    //    for (p <- sourceTargetPathTuple) println(p)

    // 分布式拷贝
    distCpFile()
  }
}
