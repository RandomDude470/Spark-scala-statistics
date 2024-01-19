import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val dataPath = "C:/Users/Yassir/Downloads/dataset obesity/ObesityDataSet.csv"
    val Spark = SparkSession.builder().appName("Spark shell").master("local[*]").getOrCreate()
    import Spark.implicits._

    val oned = new OneDiemensionalStatCsv(dataPath,",",Spark)
    val twod = new TwoDimensionalStatCsv(dataPath,",",Spark)
    val viz = new DataVisualisation(dataPath,",",Spark)


    viz.scatterPlot("Age","Weight")
//    val quali: Seq[String] = Seq(
//      "Gender",
//      "family_history_with_overweight",
//      "FAVC",
//      "CAEC",
//      "SMOKE",
//      "SCC",
//      "CALC",
//      "MTRANS",
//      "NObeyesdad"
//    )
//    val quanti: Seq[String] = Seq(
//      "Age",
//      "Height",
//      "Weight",
//      "FCVC",
//      "NCP",
//      "CH2O",
//      "FAF",
//      "TUE"
//    )

//      var means :Seq[(String, Double)] = Seq()
//      for (i<-quanti.indices){
//        means = means++ Seq((quanti(i),oned.GetMeanOf(quanti(i))))
//      }
//      means.toDF("Col","means").show()
//      var Max :Seq[(String, Double)] = Seq()
//      for (i<-quanti.indices){
//        Max = Max++ Seq((quanti(i),oned.MaxOf(quanti(i))))
//      }
//      Max.toDF("Col","Max").show()
//
//      var Mins :Seq[(String, Double)] = Seq()
//      for (i<-quanti.indices){
//        Mins = Mins++ Seq((quanti(i),oned.MinOf(quanti(i))))
//      }
//      Mins.toDF("Col","Mins").show()
//
//
//
//      var medians :Seq[(String, Double)] = Seq()
//      for (i<-quanti.indices){
//        medians = medians++ Seq((quanti(i),oned.GetQuantileOf(quanti(i),0.5)))
//      }
//      medians.toDF("Col","medians").show()
//
//      var quantiles: Seq[(String, Double,Double)] = Seq()
//      for (i <- quanti.indices) {
//        quantiles = quantiles ++ Seq((quanti(i), oned.GetQuantileOf(quanti(i), 0.25),oned.GetQuantileOf(quanti(i), 0.75)))
//      }
//      quantiles.toDF("Col","Q1","Q3").show()
//
//      var stdd: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        stdd = stdd ++ Seq((quanti(i), oned.StdDeviation(quanti(i))))
//      }
//      stdd.toDF("Col","stdd").show()
//
//      var skw: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        skw = skw ++ Seq((quanti(i), oned.Skewness(quanti(i))))
//      }
//      skw.toDF("Col","skw").show()
//
//      var kurt: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        kurt = kurt ++ Seq((quanti(i), oned.Kurtosis(quanti(i))))
//      }
//      kurt.toDF("Col","kurt").show()
//
//      var modes: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        modes = modes ++ Seq((quanti(i), oned.GetModeOf(quanti(i)).toDouble))
//      }
//      modes.toDF("Col", "modes").show()
//
//      var iqr: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        iqr = iqr ++ Seq((quanti(i), oned.IQR(quanti(i))))
//      }
//      iqr.toDF("Col", "iqr").show()
//
//      var vars: Seq[(String, Double)] = Seq()
//      for (i <- quanti.indices) {
//        vars = vars ++ Seq((quanti(i), oned.Var(quanti(i))))
//      }
//      vars.toDF("Col", "var").show()

//      var modes: Seq[(String,String)] = Seq()
//      for (i <- quali.indices) {
//        modes = modes ++ Seq((quali(i), oned.GetModeOf(quali(i))))
//      }
//      modes.toDF("Col", "modes").show()



//    var QualiPairs : Seq[(String,String)] = Seq()
//    for (i<-quali){
//      QualiPairs = QualiPairs ++ Seq((i,"NObeyesdad"))
//    }
//    for (i<-QualiPairs){
//      println(s"Crosstab for (${i._1} ,${i._2}) ")
//      twod.CrossTab(i._1,i._2).show()
//      println(s"marginal dist for (${i._1} ,${i._2}) ")
//      twod.MarginalDist(i._1).show()
//      twod.MarginalDist(i._2).show()
//      println(s"cond dist for (${i._1} ,${i._2}) ")
//      twod.ConditionalDist(i._1,i._2).show()
//      println(s"chisqtest for (${i._1} ,${i._2}) ")
//      var chires = twod.ChiSqStringCols(i._1,i._2)
//      println(s"pValues = ${chires.getAs[Vector](0)(0)}")
//      println(s"degreesOfFreedom ${chires.getSeq[Int](1).mkString("[", ",", "]")}")
//      println(s"statistics ${chires.getAs[Vector](2)}")
//
//    }
//    var QuantiPairs : Seq[(String,String)] = Seq()
//    for (i <- quanti) {
//        if(i != "Weight") {
//            QuantiPairs = QuantiPairs ++ Seq((i,"Weight"))
//        }
//    }

//    for (i <- QuantiPairs) {
//            println(s"Corr for (${i._1} _1,${i._2}) ")
//            twod.Correlation(true,i._1,i._2).show()
//    }

    
  }
}