import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import util.TextProcessTools

/*
 * @desc 文本处理测试
 * @author lirb
 * @datetime 2017/12/20,22:49
 */
class TextProcessTest extends FunSuite{

  val FILE_PATH = "E:\\Work\\DATASET\\TextData\\TheValleyofFear.txt"
  val FILE_OUT_PATH = "E:\\Work\\DATASET\\TextData\\result_temp\\"


  test("WordCount"){
    val sc = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate().sparkContext
    val textRDD = sc.textFile(FILE_PATH,2) //设定为两个分区
    val words = textRDD.flatMap( line => line.split(" "))
    val wordPairs = words.map( word => (word, 1))
    val wordCount = wordPairs.reduceByKey( (v1, v2) => v1+v2)
    wordCount.saveAsTextFile(FILE_OUT_PATH+"WC") //触发Action
  }


  test("WordCount 按照词频降序排序"){
    val sc = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate().sparkContext
    val textRDD = sc.textFile(FILE_PATH,2)
    val words = textRDD.flatMap( line => line.split(" "))
      .filter( s => !(TextProcessTools.STOP_WORDS.contains(s)||s.equals("")))
    val wordPairs = words.map( word => (word, 1)).reduceByKey( (v1,v2)=> v1+v2).filter( kv => kv._2>1) //过滤低频词
    val wordPairsVers = wordPairs.map( kv=>(kv._2, kv._1))
    val wordsSortedByValue = wordPairsVers.sortByKey(false,1)
    val wordsKV = wordsSortedByValue.map(x => (x._2,x._1)) //交换kv顺序
    wordsKV.saveAsTextFile(FILE_OUT_PATH+"WC_SORT1")
  }



  test("WordCount 按照词频降序排序+"){
    val sc = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate().sparkContext
    val textRDD = sc.textFile(FILE_PATH,2)
    val words = textRDD.flatMap( line => line.split(" "))
      .filter( s => !(TextProcessTools.STOP_WORDS.contains(s)||s.equals("")))
    val wordPairs = words.map( word => (word, 1)).reduceByKey( (v1,v2)=> v1+v2).filter( kv => kv._2>1) //过滤低频词
    val wordPairsSorted = wordPairs.sortBy( x => x._2,false,1)
    wordPairsSorted.saveAsTextFile(FILE_OUT_PATH+"WC_SORT2")

  }

}
