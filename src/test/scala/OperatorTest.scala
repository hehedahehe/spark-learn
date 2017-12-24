import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/*
 * @desc 测试RDD算子
 * @author lirb
 * @datetime 2017/12/24,15:16
 */
class OperatorTest extends FunSuite{

  private val spark = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
  private val sparkContext = spark.sparkContext

  test("RDD.mapPartitions Test"){
    val rdd = sparkContext.makeRDD(1 to 5, 2)
    rdd.mapPartitions{ iterator => {
        while(iterator.hasNext){
          println(iterator.next()) //打印不出结果
        }
        iterator
      }
    }
  }

  test("RDD.mapPartionsWithIndex"){
    val rdd =sparkContext.makeRDD(1 to 5, 2)
    val rdd2 = rdd.mapPartitionsWithIndex( (index, iterator) => {
      println("对分区" + index + "进行操作。。。")
      val list = iterator.toList
      list.map( x => x*2).toIterator
    })
    val res = rdd2.collect()
    res.foreach(println)

  }


}
