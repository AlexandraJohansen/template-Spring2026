import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q1 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doRetail(myrdd)
    saveit("spark1output", result)
  }

  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Q1").setIfMissing("spark.master", "local[*]")
    new SparkContext(conf)
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("/datasets/retailtab")
  }

  def doRetail(input: RDD[String]): RDD[(String, Int)] = {
    // Drop the header line
    val noHeader = input.filter(line => !line.startsWith("InvoiceNo"))

    // Parse each line; fields: InvoiceNo=0, StockCode=1, Description=2,
    //   Quantity=3, InvoiceDate=4, UnitPrice=5, CustomerID=6, Country=7
    val parsed = noHeader
      .map(line => line.split("\t", -1))
      .filter(fields => fields.length >= 8)
      .map(fields => (fields(7).trim, fields(2).trim))  // (country, description)

    // Every country must appear in output even with count 0.
    // Emit (country, 0) for every line to establish all countries,
    // then union with (country, 1) only for lines where description has "ER".
    val allCountries = parsed.map({ case (country, _)    => (country, 0) })
    val erLines      = parsed
      .filter({ case (_, desc) => desc.contains("ER") })
      .map({ case (country, _) => (country, 1) })

    allCountries
      .union(erLines)
      .reduceByKey(_ + _)
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val lines = Seq(
      "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
      "536365\tA\tWHITE HANGING HEART\t6\t2010\t2.55\t17850\tUnited Kingdom",  // no ER
      "536365\tB\tERASER SET\t3\t2010\t1.25\t17850\tUnited Kingdom",           // has ER
      "536366\tC\tSILVER TRAY\t2\t2010\t3.39\t17851\tFrance",                  // no ER
      "536366\tD\tSUPER ERASER\t4\t2010\t2.10\t17851\tFrance",                 // has ER
      "536367\tE\tWOODEN BOX\t5\t2010\t1.50\t17852\tGermany"                   // no ER; Germany must still appear with 0
    )
    sc.parallelize(lines)
  }

  def expectedOutput(sc: SparkContext): RDD[(String, Int)] = {
    sc.parallelize(Seq(
      ("United Kingdom", 1),
      ("France",         1),
      ("Germany",        0)
    ))
  }

  def saveit(name: String, counts: RDD[(String, Int)]) = {
    counts.saveAsTextFile(name)
  }
}
