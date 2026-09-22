import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Q3 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doRetail(myrdd)
    saveit("spark3output", result)
  }

  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Q3").setIfMissing("spark.master", "local[*]")
    new SparkContext(conf)
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("/datasets/retailtab")
  }

  def doRetail(input: RDD[String]): RDD[(String, (Int, Int))] = {
    // Drop the header line
    // Fields: InvoiceNo=0, StockCode=1, Description=2,
    //         Quantity=3, InvoiceDate=4, UnitPrice=5, CustomerID=6, Country=7
    val parsed = input
      .filter(line => !line.startsWith("InvoiceNo"))
      .map(line => line.split("\t", -1))
      .filter(fields => fields.length >= 8 && fields(6).trim.nonEmpty)
      .map(fields => {
        val country    = fields(7).trim
        val customerID = fields(6).trim
        val quantity   = fields(3).trim.toInt
        (country, (customerID, quantity))  // (country, (customerID, qty))
      })

    // aggregateByKey to accumulate:
    //   - a Set[String] of distinct customerIDs (no .distinct() allowed)
    //   - running total quantity
    // zero value: (empty set, 0)
    // seqOp:  fold one line's (customerID, qty) into the accumulator
    // combOp: merge two partition accumulators together
    parsed
      .aggregateByKey((Set[String](), 0))(
        { case ((custSet, totalQty), (cust, qty)) => (custSet + cust, totalQty + qty) },
        { case ((set1, qty1), (set2, qty2))       => (set1 ++ set2,  qty1 + qty2)    }
      )
      // Replace the set with its size for the final (Int, Int) output
      .map({ case (country, (custSet, totalQty)) => (country, (custSet.size, totalQty)) })
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    val lines = Seq(
      "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
      "536365\tA\tWHITE TRAY\t3\t2010\t2.55\t17850\tUnited Kingdom",  // UK, cust 17850, qty 3
      "536365\tB\tERASER SET\t4\t2010\t1.25\t17850\tUnited Kingdom",  // UK, cust 17850 again, qty 4
      "536366\tC\tSILVER BOX\t6\t2010\t3.39\t17851\tUnited Kingdom",  // UK, cust 17851, qty 6
      "536367\tD\tGLASS CUP\t2\t2010\t1.50\t17852\tFrance"            // France, cust 17852, qty 2
    )
    sc.parallelize(lines)
  }

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
    // UK:     2 distinct customers (17850, 17851), total qty = 3+4+6 = 13
    // France: 1 distinct customer  (17852),        total qty = 2
    sc.parallelize(Seq(
      ("United Kingdom", (2, 13)),
      ("France",         (1, 2))
    ))
  }

  def saveit(name: String, counts: RDD[(String, (Int, Int))]) = {
    counts.saveAsTextFile(name)
  }
}
