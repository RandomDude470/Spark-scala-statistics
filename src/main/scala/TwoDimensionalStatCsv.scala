import breeze.linalg.Matrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation.corr
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.types._


class TwoDimensionalStatCsv(path: String,sep:String,Spark:SparkSession) {
//  private val Spark = SparkSession.builder().appName("Spark shell").master("local[*]").getOrCreate()
//  Spark.sparkContext.setLogLevel("ERROR")
//  Logger.getLogger("akka").setLevel(Level.ERROR)
//  Logger.getLogger("org").setLevel(Level.ERROR)
  private val df = Spark.read.format("csv").option("header", "true").option("delimiter", sep).option("encoding", "UTF-8").load(path)

  import Spark.implicits._


  def CrossTab(Column1: String, Column2: String, Margins: Boolean = false): DataFrame = {
    if (!Margins) {
      val crosstab = df.stat.crosstab(Column1, Column2)
      crosstab.show()
      crosstab
    } else {
      var crosstab = df.stat.crosstab(Column1, Column2)
      val c = crosstab.columns.slice(1, crosstab.columns.length).map(c => col(c)).reduce((a, b) => a + b)
      crosstab.withColumn("Total", c).show()
      crosstab = crosstab.withColumn("Total", c)
      var total_cols = crosstab.groupBy().sum(crosstab.columns.slice(1, crosstab.columns.length): _*)
      total_cols = total_cols.withColumn("Total", lit("Total"))
      val total_cols_order = Array("Total") ++ total_cols.columns.slice(0, total_cols.columns.length - 1)
      total_cols = total_cols.select(total_cols_order.map(x => col(x)): _*)
      crosstab = crosstab.union(total_cols)
      crosstab.show()
      crosstab

    }

  }

  def ConditionalDist(Column1: String, Column2: String): DataFrame = {
    var crosstab = df.stat.crosstab(Column1, Column2)
    val numericColumns = crosstab.columns.slice(1, crosstab.columns.length)
    val columns = crosstab.columns.clone()
    val head = crosstab.select(col(columns(0)))
    crosstab = crosstab.select(numericColumns.map(c => col(c) / df.count()): _*)
    val con = new Utils()
    crosstab = con.concat(head, crosstab)


    crosstab.show()
    crosstab
  }

  def MarginalDist(ColumnName: String): DataFrame = {
    val count = df.select(col(ColumnName)).groupBy(ColumnName).count()
    val dist = count.select(col(ColumnName), col("count") / df.count)
    dist
  }

  def ChiSqStringCols(Column1: String, Column2: String): Row = {
    val col1 = df.select(col(Column1)).collect.map(c => c.getString(0))
    val col2 = df.select(col(Column2)).collect.map(c => c.getString(0))
    val utils = new Utils()

    val indexedColumn1 = utils.Indexer(col1)
    val indexedColumn2 = utils.Indexer(col2).map(c => Vectors.dense(c.toDouble))


    var df1 = indexedColumn1.toSeq.toDF("label")
    //    df1 = utils.concat(df1,indexedColumn2.toSeq("features"))
    var a = Array[(Int, Vector)]()
    for (i <- indexedColumn1.indices) {
      a = a :+ (indexedColumn1(i), indexedColumn2(i))
    }
    df1 = a.toSeq.toDF("label", "features")

    val chiSquareTestResult = ChiSquareTest.test(df1, "features", "label").head()

    chiSquareTestResult
  }

  def Correlation(Labels:Boolean,Columns: String*): DataFrame = {

    if (Labels)
      {
        val assembler = new VectorAssembler()
          .setInputCols(Columns.toArray)
          .setOutputCol("features")
        val trdf = assembler.transform(df.select(Columns.map(c => col(c).cast("Double")): _*))
        val Row(cof: DenseMatrix) = corr(trdf, "features", "pearson").head
        val cor = cof.toArray
        val utils = new Utils
        var dfdata = Columns.toDF("Correlation")

        var s = Seq[Double]()
        val index = cor.length / Columns.length
        for (i <- 0 until index) {
          s = Seq()
          for (j <- 0 until index) {
            s = s ++ Seq(cor(j + index * i))
          }
          //s.toDF().show()
          dfdata = utils.concat(dfdata, s.toDF(Columns(i)))

        }
        dfdata
      }
      else{
      val assembler = new VectorAssembler()
        .setInputCols(Columns.toArray)
        .setOutputCol("features")
      val trdf = assembler.transform(df.select(Columns.map(c => col(c).cast("Double")): _*))
      val res = corr(trdf, "features", "spearman")
      res
    }

  }


}
