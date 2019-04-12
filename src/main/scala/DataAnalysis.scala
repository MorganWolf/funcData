import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HelloScala extends App{

  val pathToFile = "src/data.txt"

  /**
    *  Load the data from the text file and return an RDD of words
    */
  def loadData(): RDD[String] = {
    val conf = new SparkConf()
      .setAppName("Analysis")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(pathToFile)
      .flatMap(_.split("\n"))
  }

  def wordcount(): RDD[(String, Int)] = {
    loadData()
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  def splitByParameter(nb: Int): RDD[String] = {
    loadData()
      .map(_.split(":"))
      .filter()
  }


  def distinctBus(): RDD[String] = {
    splitByParameter(2)
      .distinct()
  }

  wordcount().foreach(println)
  splitByParameter(2).foreach(println)
  distinctBus().foreach(println)

}