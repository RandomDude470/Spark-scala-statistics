import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.functions._



class OneDiemensionalStatCsv(path : String,sep:String,Spark:SparkSession) {
//  private val Spark = SparkSession.builder().appName("NewSparkSession").master("local[*]").getOrCreate()
//  Spark.sparkContext.setLogLevel("ERROR")
//  Logger.getLogger("akka").setLevel(Level.ERROR)
//  Logger.getLogger("org").setLevel(Level.ERROR)
  private val df = Spark.read.format("csv").option("header", "true").option("delimiter",sep).option("encoding","UTF-8").load(path)



  def GetAttributes(): Array[String] = {
    df.printSchema()
    df.columns
  }

  def GetMeanOf( ColumnName : String):Double = {
    val res = df.select(mean(ColumnName)).first().getDouble(0)
    BigDecimal(res).setScale(2, BigDecimal.RoundingMode.DOWN).toDouble
  }

  def GetQuantileOf(columnName: String, Q:Double ):Double ={
    val Column = df.select(col(columnName).cast("double"))
    val resultArr = Column.stat.approxQuantile(columnName,Array(Q),0.01)
    if (resultArr.isEmpty){
      throw new Error("Empty result. Data might be Non Numeric")
    }else{
      val res = resultArr(0)
      BigDecimal(res).setScale(2, BigDecimal.RoundingMode.DOWN).toDouble
    }
  }

  def GetModeOf(columnName: String):String={
    df.groupBy(columnName).count().orderBy(desc("count")).first()(0).toString
  }

  def MaxOf(columnName: String):Double ={
    df.select(max(columnName)).first().getString(0).toDouble
  }

  def MinOf(columnName: String): Double = {
    df.select(min(columnName)).first().getString(0).toDouble
  }

  def IQR(columnName: String): Double = {
    val Q3 = GetQuantileOf(columnName, 0.75)
    val Q1 = GetQuantileOf(columnName, 0.25)
    Q3 - Q1
  }

  def StdDeviation(columnName: String): Double = {
    df.select(stddev(columnName)).first().getDouble(0)
  }

  def Var(columnName: String): Double = {
    val sigma = df.select(stddev(columnName)).first().getDouble(0)
    sigma*sigma
  }

  def Skewness(columnName: String): Double = {
    df.select(skewness(columnName)).first().getDouble(0)
  }

  def Kurtosis(columnName: String): Double = {
    df.select(kurtosis(columnName)).first().getDouble(0)
  }
  
}
