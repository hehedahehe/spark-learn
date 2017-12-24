package myfuncs

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import util.MyFunctions

/*
 * @desc
 * @author lirb
 * @datetime 2017/12/22,14:11
 */
class MyFunctionsTest extends FunSuite{

  private val spark = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
  private val sparkContext = spark.sparkContext

  test("test parse linkage"){

    val line = "39086,47614,1,?,1,?,1,1,1,1,1,TRUE"
    val md = MyFunctions.parseLineKage(line)
    println(md)

  }



  test("存放一些脚本"){
    val ch2 = sparkContext.textFile("hdfs:///user/xlhadoop/dataset/chapter2Sample")
  }


  test("statsWithMissing"){
    val a1 = Array(0.1, 1, 1, 1.0)
    val a2 = Array(0.1, 0.2, 0.3, 5)
    val a3 : Array[Array[Double]] = Array(a1,a2)
    val rdd = sparkContext.parallelize(a3) //因为master设置为local[2]，即分配为两个核，默认分区为分配的核数
//    val rdd = sparkContext.parallelize(a3,1) //设置为一个分区
    val stats = MyFunctions.statsWithMissing(rdd)
    stats.foreach(println)
  }

}
