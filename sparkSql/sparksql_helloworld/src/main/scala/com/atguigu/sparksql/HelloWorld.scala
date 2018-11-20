package com.atguigu.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
  * Created by wuyufei on 31/07/2017.
  */
object HelloWorld {

  val logger = LoggerFactory.getLogger(HelloWorld.getClass)

  def main(args: Array[String]) {


    //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // 通过引入隐式转换可以将RDD的操作添加到 DataFrame上。
    import spark.implicits._

    //通过spark.read操作读取JSON数据。
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to
    //show操作类似于Action，将DataFrame直接打印到Console。
    df.show()

    // DSL风格的使用方式中，属性的获取方法
    df.filter($"age" > 21).show()

    // 将DataFrame注册为一张临时表
    df.createOrReplaceTempView("persons")

    // 通过spark.sql方法来运行正常的SQL语句。
    spark.sql("SELECT * FROM persons where age > 21").show()

    // 关闭整个SparkSession。
    spark.stop()
  }

}


