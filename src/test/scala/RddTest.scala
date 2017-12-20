import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class RddTest extends FunSuite {


  test("An empty Set should have size 0") {
    assert(Set.empty.size == 0)
  }



  val CHAPTER2_DATA_DIR_ROOT : String = "E:\\WORK\\DATA_SET\\chapter2"
  val MASTER : String = "local[2]"
  val APP_NAME = "Chapter2"

  test("Test Cache：这种缓存不起作用"){

    val spark = SparkSession.builder.master(MASTER).appName(APP_NAME).getOrCreate()

    val sparkContext = spark.sparkContext

    val blocks = sparkContext.textFile(CHAPTER2_DATA_DIR_ROOT)
//    blocks.cache()
    val count = blocks.count()
    println("Count ==> " + count)

    blocks.cache()

    blocks.map(x=>(x,1)).repartition(3).reduceByKey(_+_,3)

    val countFromCached = blocks.count()
    println("countFromCached ==> " + countFromCached)


    spark.stop()
  }


  test("Test Cache 2 "){
    val spark = SparkSession.builder.master(MASTER).appName(APP_NAME).getOrCreate()

    val sparkContext = spark.sparkContext

    val blocks = sparkContext.textFile(CHAPTER2_DATA_DIR_ROOT)
  }






}

