import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Tri {

  def main(args: Array[String]) = { // this is the entry point to our code
    val sc = getSC() // one function to get the sc var
    val myrdd = getFB(sc) // on function to get the rdd
    val counts = countTriangles(myrdd) // get the number of tri
    // sadly we convert this single number into an rdd and save it to HDFS
    sc.parallelize(List(counts), 1).saveAsTextFile("NumberOfTriangles")
  }

  def getSC(): SparkContext = { // get the spark context variable
    val conf = new SparkConf().setAppName("TriangleCount")
    new SparkContext(conf)
  }

  def getFB(sc: SparkContext): RDD[(String, String)] = {
    
    val text = sc.textFile("/datasets/facebook")
    text.map(line => {
      val parts = line.split(" ")
      (parts(0), parts(1))
    })
  }

  def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
    edgeList.flatMap { case (a, b) => Seq((a, b), (b, a)) }.distinct()
  }

  def noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
    // 1 transformation (filter out self-loops)
    edgeList.filter { case (a, b) => a != b }
  }

  def friendsOfFriends(edgeList: RDD[(String, String)]): RDD[(String, (String, String))] = {
 
    val flipped = edgeList.map { case (x, y) => (y, x) }
    flipped.join(edgeList)
  }

  def journeyHome(
      edgeList: RDD[(String, String)],
      twoPaths: RDD[(String, (String, String))]
  ): RDD[((String, String), (String, Null))] = {
  
    val pathsByEndpoints = twoPaths.map { case (y, (x, z)) => ((x, z), y) }
    val edgesAsKeys = edgeList.map { case (x, z) => ((x, z), null.asInstanceOf[Null]) }

    edgesAsKeys.join(pathsByEndpoints).map { case ((x, z), (nullVal, y)) =>
      ((x, z), (y, nullVal))
    }
  }

  def toyGraph(sc: SparkContext): RDD[(String, String)] = {
    val mylist = List[(String, String)](
      ("1", "2"), ("2", "1"), ("2", "3"), ("3", "2"),
      ("1", "3"), ("3", "1"), ("1", "4"), ("4", "1"),
      ("4", "3"), ("3", "4"), ("3", "5"), ("5", "3"),
      ("1", "3"), ("3", "1"), ("1", "1"), ("3", "5"),
      ("5", "3"), ("1", "3"), ("3", "1"), ("1", "4"),
      ("4", "1"), ("4", "3")
    )
    sc.parallelize(mylist, 2)
  }

  def countTriangles(edgeList: RDD[(String, String)]): Long = {
    val no_self_edges = noSelfEdges(edgeList)
    val double_it = makeRedundant(no_self_edges)
    val fr = friendsOfFriends(double_it)
    val almostThere = journeyHome(double_it, fr)

    almostThere.count() / 6
  }
}

