package util

import org.apache.spark.util.StatCounter

/*
 * @desc 统计计数器
 * @author lirb
 * @datetime 2017/12/22,15:44
 */
class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if(java.lang.Double.isNaN(x)){
      missing += 1
    }else{
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    this.stats.merge(other.stats)
    this.missing += other.missing
    this
  }

  override def toString: String = {
    "STATUS:" + this.stats + " " + "MISSING:" + this.missing
  }

}

object NAStatCounter extends Serializable{
  def apply(x: Double) = new NAStatCounter().add(x)

}


