import org.scalatest.FunSuite
import util.NAStatCounter
/*
 * @desc
 * @author lirb
 * @datetime 2017/12/22,15:58
 */
class StatusCounterTest extends FunSuite{

  test("测试StatusCounter"){
    val data = List(1.0,Double.NaN,19.1,20.0)
    val statCounter = NAStatCounter(0)
//    data.foreach( x => statCounter.add(x)) //这样做感觉有些
    println(statCounter.toString)

    val nas1 = Array(1.0, Double.NaN).map(d => NAStatCounter(d))
    println(nas1.toString)
  }


}
