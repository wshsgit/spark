package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
  * Created by wuyufei on 31/07/2017.
  */
object CdnStatics {

  val logger = LoggerFactory.getLogger(CdnStatics.getClass)

  //匹配IP地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CdnStatics")

    val sc = new SparkContext(conf)

    val input = sc.textFile("C:\\Users\\Administrator\\Desktop\\【尚硅谷】大数据技术之Spark\\3.code\\spark\\sparkCore\\sparkcore_cdn\\src\\main\\resources\\cdn.txt").cache()

    //统计独立IP访问量前10位
    ipStatics(input)

    //统计每个视频独立IP数
    videoIpStatics(input)

    //统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {

    def isMatch(pattern: Regex, str: String) = {
      str match {
        case pattern(_*) => true
        case _ => false
      }
    }

    /**
      * 获取日志中小时和http 请求体大小
      *
      * @param line
      * @return
      */
    def getTimeAndSize(line: String) = {
      var res = ("", 0L)
      try {
        val httpSizePattern(code, size) = line
        val timePattern(year, hour) = line
        res = (hour, size.toLong)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
      res
    }

    //3.统计一天中每个小时间的流量
    data.filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
      .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))
  }

  // 统计每个视频独立IP数
  def videoIpStatics(data: RDD[String]): Unit = {
    def getFileNameAndIp(line: String) = {
      (videoPattern.findFirstIn(line).mkString, IPPattern.findFirstIn(line).mkString)
    }
    //2.统计每个视频独立IP数
    data.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).map(x => getFileNameAndIp(x)).groupByKey().map(x => (x._1, x._2.toList.distinct)).
      sortBy(_._2.size, false).take(10).foreach(x => println("视频：" + x._1 + " 独立IP数:" + x._2.size))
  }


  // 统计独立IP访问量前10位
  def ipStatics(data: RDD[String]): Unit = {

    //1.统计独立IP数
    val ipNums = data.map(x => (IPPattern.findFirstIn(x).get, 1)).reduceByKey(_ + _).sortBy(_._2, false)

    //输出IP访问数前量前10位
    ipNums.take(10).foreach(println)

    println("独立IP数：" + ipNums.count())
  }

}