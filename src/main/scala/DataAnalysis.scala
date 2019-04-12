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
      .setAppName("Wordcount")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(pathToFile)
      .flatMap(_.split("\n"))
  }

  def wordcount(): RDD[(String, Int)] = {
    loadData()
      .map(word => (word, 1))
      .reduceByKey(_ + _)
  }

  wordcount().foreach(println)


}