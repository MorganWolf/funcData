import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HelloScala extends App{

  val pathToFile = "../FunctionnalDataProgramming-master/src/main/resources/result.txt"

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

  def splitByParameter(nb: Int): RDD[String]  = {
    loadData()
      .map(_.split(':')(nb))
  }

  /** return : chauffeur, type_accident, nombre */
  def accidentParChauffeur(nb: Int): RDD[((String, String), Int)]  = {
    loadData()
      .map(_.split(':'))
      .map(x => (x(1),x(6)))
      .filter(_._2 != "Normal")
      .map(chauffeur => (chauffeur, 1))
      .reduceByKey(_ + _)
  }

  /** return :  id_Arret, nb passager */
  def passagerParArret(): RDD[(String, Int)]  = {
    loadData()
      .map(_.split(':'))
      .map(x => (x(2),x(3).toInt))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }


  /** return : id_bus, km*/
  def kmParBus(): RDD[(String, Int)] =  {
    loadData()
      .map(_.split(':'))
      .map(x => (x(0),x(5).toInt))
      .reduceByKey(math.max(_, _))
  }

  //FONCTIONS 
  //splitByParameter(1).foreach(println)
  //passagerParArret().take(1).foreach(println)
  //kmParBus().foreach(println)


}
