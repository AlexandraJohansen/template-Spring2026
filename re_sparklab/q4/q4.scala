import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q4 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doCities(myrdd)
    saveit("spark4output", result)
  }

  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Q4").setIfMissing("spark.master", "local[*]")
    new SparkContext(conf)
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("/datasets/cities")
  }

  def doCities(input: RDD[String]): RDD[(String, (Int, Int, Int))] = {
    // No header in cities dataset
    // Fields: name=0, state=1, county=2, population=3, zip=4, id=5
    // Bad lines: filter those where we can't safely parse name, state, population
    input
      .map(line => line.split("\t", -1))
      .filter(fields =>
        fields.length >= 4        &&
        fields(0).trim.nonEmpty   &&   // must have a city name
        fields(1).trim.nonEmpty   &&   // must have a state
        fields(3).trim.nonEmpty        // must have a population field to parse
      )
      .map(fields => {
        val name       = fields(0).trim
        val state      = fields(1).trim
        val population = fields(3).trim.toInt
        val hasCity    = if (name.contains("City")) 1 else 0
        val noNew      = if (!name.contains("New")) 1 else 0
        (state, (hasCity, noNew, population))
      })
      .reduceByKey({ case ((c1, n1, p1), (c2, n2, p2)) => (c1 + c2, n1 + n2, p1 + p2) })
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    // Fields: name \t state \t county \t population \t zip \t id
    val lines = Seq(
      "Kansas City\tMO\tJackson\t500000\t64101 64102\t1",   // hasCity=1, noNew=1, pop=500000
      "New York\tNY\tNew York\t8000000\t10001\t2",           // hasCity=0, noNew=0, pop=8000000
      "New York City\tNY\tNew York\t100000\t10002\t3",       // hasCity=1, noNew=0, pop=100000
      "Springfield\tMO\tGreene\t167000\t65801\t4",           // hasCity=0, noNew=1, pop=167000
      "\tBAD\tBad\t\t\t"                                     // empty name → filtered
    )
    sc.parallelize(lines)
  }

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int, Int))] = {
    // MO: Kansas City (1,1,500000) + Springfield (0,1,167000) = (1,2,667000)
    // NY: New York    (0,0,8000000) + New York City (1,0,100000) = (1,0,8100000)
    sc.parallelize(Seq(
      ("MO", (1, 2, 667000)),
      ("NY", (1, 0, 8100000))
    ))
  }

  def saveit(name: String, counts: RDD[(String, (Int, Int, Int))]) = {
    counts.saveAsTextFile(name)
  }
}
