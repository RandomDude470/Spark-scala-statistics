import scala.collection.mutable
import com.cibo.evilplot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

import scala.util.Random


object testindexer {

  def main(args: Array[String]): Unit = {
//  val hashMap = new mutable.HashMap[String,Int]()
//  var result: Array[Int] = new Array[Int](1)
//  result :+ 0
//  println(result.length)
val data = Seq(542.0,390.0)

    displayPlot(BoxPlot(Seq(Seq.fill(Random.nextInt(30))(Random.nextDouble())))
      .yAxis()
      .xAxis()
      .frame()
      .render())


  }

  def Indexer(Arr: Array[String]): Array[Int] = {
    var index = 0
    val hashMap = new mutable.HashMap[String, Int]()
    var result: Array[Int] = new Array[Int](0)
    Arr.foreach(s =>{

      if (hashMap.getOrElse(s, -1) == -1) {
        result = result :+ index

        hashMap += (s -> index)
        index += 1
      } else {
        result = result :+ hashMap.getOrElse(s, -1)
      }}
    )
    println(result.length)
    result
  }
}
