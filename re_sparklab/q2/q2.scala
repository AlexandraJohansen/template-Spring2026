import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q2 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doRetail(myrdd)
    saveit("spark2output", result)
  }

  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Q2").setIfMissing("spark.master", "local[*]")
    new SparkContext(conf)
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("/datasets/retailtab")
  }

  def doRetail(input: RDD[String]): RDD[(String, (Int, Int))] = {
    // Drop the header line
    // Fields: InvoiceNo=0, StockCode=1, Description=2,
    //         Quantity=3, InvoiceDate=4, UnitPrice=5, CustomerID=6, Country=7
    input
      .filter(line => !line.startsWith("InvoiceNo"))
      .map(line => line.split("\t", -1))
      .filter(fields => fields.length >= 8 && fields(6).trim.nonEmpty)
      .map(fields => {
        val customer = fields(6).trim
        val quantity = fields(3).trim.toInt
        val erCount  = if (fields(2).trim.contains("ER")) 1 else 0
        (customer, (quantity, erCount))  // (customerID, (qty, erCount))
      })
      // Sum both components per customer
      .reduceByKey({ case ((q1, e1), (q2, e2)) => (q1 + q2, e1 + e2) })
      // Remove customers whose total quantity is less than 5
      .filter({ case (_, (totalQty, _)) => totalQty >= 5 })
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val lines = Seq(
      "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
      "536365\tA\tERASER SET\t3\t2010\t2.55\t17850\tUK",    // cust 17850: qty=3, er=1
      "536365\tB\tWHITE TRAY\t4\t2010\t1.25\t17850\tUK",    // cust 17850: qty=4, er=0 → total qty=7, er=1
      "536366\tC\tSUPER ERASER\t6\t2010\t3.39\t17851\tUK",  // cust 17851: qty=6, er=1 → total qty=6, er=1
      "536367\tD\tWOODEN BOX\t2\t2010\t1.50\t17852\tUK"     // cust 17852: qty=2 → filtered out (qty < 5)
    )
    sc.parallelize(lines)
  }

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
    // 17850: totalQty=7, erCount=1  kept
    // 17851: totalQty=6, erCount=1  kept
    // 17852: totalQty=2             filtered out
    sc.parallelize(Seq(
      ("17850", (7, 1)),
      ("17851", (6, 1))
    ))
  }

  def saveit(name: String, counts: RDD[(String, (Int, Int))]) = {
    counts.saveAsTextFile(name)
  }
}
