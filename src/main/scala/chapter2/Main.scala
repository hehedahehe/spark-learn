package chapter2

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val dataRootPath : String = "E:\\WORK\\DATA_SET\\chapter2"

  def main(args : Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Chapter2")
    val sparkContext = new SparkContext(sparkConf)
    val blocks = sparkContext.textFile(dataRootPath).filter(x => !x.contains("id_")) //所有数据

    val demoData = blocks.take(10)

    demoData.foreach(x => println("==> " + x))

  }

}
