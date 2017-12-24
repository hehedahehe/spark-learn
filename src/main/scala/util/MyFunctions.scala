package util

import org.apache.spark.rdd.RDD

/*
 * @desc
 * @author lirb
 * @datetime 2017/12/20,23:02
 */
object MyFunctions {

  case class MatchData(id1: Int, id2: Int, scores: Array[Double], isMatched: Boolean)

  def parseLineKage(line: String): MatchData ={
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2,pieces.length-1).map(toDouble)
    val matched = pieces(pieces.length-1).toBoolean
    MatchData(id1,id2,scores,matched)
  }

  private def toDouble(item: String): Double={
    if("?".equals(item))Double.NaN else item.toDouble
  }


  def isValidLine(line: String): Boolean={
    if(line.split(",").length!=12)false else true
  }

  /**
    * 传入一个RDD，进行统计量+缺失值的分析
    * @param rdd
    * @return
    */
  def statsWithMissing(rdd : RDD[Array[Double]]): Array[NAStatCounter] = {
    //iter是每个分区的数据，mapPartitions的作用是对每个分区的数据进行操作（可能有些操作
    // 需要对一个分区数据分配公用的资源，比如数据库、网络连接等，分区内的数据可以实现资源共享）
    //而若是直接调用map操作的话，各个元素的资源都是独立的
    val nastats = rdd.mapPartitions( (iter: Iterator[Array[Double]]) =>{
        val a1 : Array[NAStatCounter] = iter.next().map( x => NAStatCounter(x)) //第一条记录各项的统计数据
        iter.foreach( arr =>{
//          a1.zip(arr).foreach( case(n, d) => n.add(d)) //{换成(就不行了？
          a1.zip(arr).foreach{ case (n,d) => n.add(d)}
        })
        Iterator(a1)
      }
    )
    println("  --")
    //nastats的reduce是针对各个分区的数据进行合并操作，若分区数量为1，则会不进行Reduce操作
    //nastats的类型是：MapPartitionsRDD
    //rdd的类型是：ParallelCollectionRDD
    nastats.reduce( (n1,n2) => {
      n1.zip(n2).map{ case(a, b) => a.merge(b)}
    })

  }

}
