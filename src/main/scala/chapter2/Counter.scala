package chapter2


class Counter extends Serializable {

  private var isHeaderCounter: Long = 0
  private var isNotHeaderCounter: Long = 0

  def addHeader(): Unit ={
    this.isHeaderCounter += 1
  }

  def addNotHeader(): Unit ={
    this.isNotHeaderCounter += 1
  }


  def getHeaderCount(): Long ={
    this.isHeaderCounter
  }

  def getNotHeaderCount(): Long ={
    this.isNotHeaderCounter
  }

}


object Counter {
  def countHeader(item:String, counter: Counter): Unit = {
    if(item.contains("id_")){
      counter.addHeader()
    }else{
      counter.addNotHeader()
    }
  }
}

