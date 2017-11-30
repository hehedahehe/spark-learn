package chapter2

import org.apache.spark.sql.SparkSession
import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val dataRootPath : String = "E:\\WORK\\DATA_SET\\chapter2"

  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder.appName("Chapter2").getOrCreate()

    val sparkContext = spark.sparkContext

    val accumulator = sparkContext.longAccumulator("My Accumulator")
    val blocks = sparkContext.textFile(dataRootPath)

//    val counter = new Counter
//    blocks.foreach(x => Counter.countHeader(x, counter))
//    println("header=>"+counter.getHeaderCount())
//    println("not header=>"+counter.getNotHeaderCount())


    blocks.foreach( x => if (x.contains("id_")) accumulator.add(1))
    println("Header ==> " + accumulator.count)
    println("Not Header ==> " + (blocks.count() - accumulator.count ))

    spark.stop()


  }



}
