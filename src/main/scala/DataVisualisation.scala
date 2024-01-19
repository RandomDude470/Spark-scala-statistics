import com.cibo.evilplot._
import com.cibo.evilplot.numeric.Point
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._


class DataVisualisation(path :String,sep:String,Spark :SparkSession) {
//  private val Spark = SparkSession.builder().appName("Spark shell").master("local[*]").getOrCreate()
//  Spark.sparkContext.setLogLevel("ERROR")
//  Logger.getLogger("akka").setLevel(Level.ERROR)
//  Logger.getLogger("org").setLevel(Level.ERROR)
  private val df = Spark.read.format("csv").option("header", "true").option("delimiter",sep).option("encoding", "UTF-8").load(path)

  def barChart(Column : String) : Unit={
    val col = df.select(Column).groupBy(Column).count()
    val values =col.select("count").rdd.flatMap(r => r.toSeq).collect().map {
      case value: Double => value
      case value: Int => value.toDouble
      case value: Long => value.toDouble
      case value: String => value.toDouble
      case value => {
        println(value.getClass)
        throw new Exception("type error values")
      }
    }.toSeq
    val labels =col.select(Column).rdd.flatMap(r => r.toSeq).collect().map {
      case value: String => value
      case _ => throw new Exception("type error labels")
    }

    println(values.mkString("Array(", ", ", ")"))
    displayPlot(BarChart(values)
      .standard(xLabels = labels)
      .ybounds(0,values.max)
      .render())

  }

  def pieChart(Column: String): Unit = {
    val col = df.select(Column).groupBy(Column).count()
    val values = col.select("count").rdd.flatMap(r => r.toSeq).collect().map {
      case value: Double => value
      case value: Int => value.toDouble
      case value: Long => value.toDouble
      case value: String => value.toDouble
      case value => {
        println(value.getClass)
        throw new Exception("type error values")
      }
    }.toSeq
    val labels = col.select(Column).rdd.flatMap(r => r.toSeq).collect().map {
      case value: String => value
      case _ => throw new Exception("type error labels")
    }
    var s : Seq[(String , Double)] = Seq()
    for (i <- values.indices){
      s = s ++ Seq("" -> values(i) )
    }

    println(s)
    displayPlot(PieChart(s)
      .rightLegend(labels = Some(labels.toSeq))
      .render()
    )


  }
  def boxPlot(Column :String,quali:Boolean = true):Unit = {
    if (quali) {
      val column = df.select(col(Column)).collect.map(c => c.getString(0))
      val utils = new Utils()
      val values = Seq(utils.Indexer(column).map(i => i.toDouble).toSeq)
      displayPlot(
        BoxPlot(values)
          .xAxis()
          .yAxis()
          .frame()
          .render()
      )
    }
    else{
      val column = df.select(col(Column)).collect.map(c => c.getString(0))
      val values = Seq(column.map(i => i.toDouble).toSeq)
      displayPlot(
        BoxPlot(values)
          .xAxis()
          .yAxis()
          .frame()
          .render()
      )
    }
  }

  def scatterPlot(Column1: String, Column2: String): Unit = {
    val col1 = df.select(col(Column1)).collect.map(c => c.getString(0).toDouble)
    val col2 = df.select(col(Column2)).collect.map(c => c.getString(0).toDouble)
    var readyData :Seq[Point] = Seq()
    for (i<-col1.indices){
      readyData = readyData ++ Seq(Point(col1(i),col2(i)))
    }
    displayPlot(
      ScatterPlot(readyData)
        .standard()
        .xLabel("x")
        .yLabel("y")
        .rightLegend()
        .render()
    )
  }
}
