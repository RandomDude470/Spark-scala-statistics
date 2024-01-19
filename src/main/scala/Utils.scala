import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable



class Utils {
  def concat(df1 : DataFrame,df2:DataFrame):DataFrame = {
    val w1 = Window.orderBy("counter")
    val w2 = Window.orderBy("counter")
    var df1withid = df1.withColumn("counter",monotonically_increasing_id())
    var df2withid = df2.withColumn("counter",monotonically_increasing_id())

    df1withid = df1withid.withColumn("id" , row_number().over(w1)).drop("counter")
    df2withid = df2withid.withColumn("id" , row_number().over(w2)).drop("counter")
    df1withid.join(df2withid,"id").drop("id")
  }
  def Indexer(Arr  : Array[String]) : Array[Int] = {
    var index = 0
    val hashMap = new mutable.HashMap[String,Int]()
    var result : Array[Int] = new Array[Int](0)
    Arr.foreach( s =>
      if (hashMap.getOrElse(s, -1)== -1){
        result = result:+index
        hashMap += (s -> index)
        index+=1
      }else{
        result = result:+hashMap.getOrElse(s, -1)
      }
    )

    result
  }
}
