import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Q2 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dataframes_q2")  // save the rdd to your home directory in HDFS
    }

    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        // Handle null customer_id by filling with "blank"
        val custFilled = customers.na.fill("blank", Seq("customer_id"))
        val ordFilled = orders.na.fill("blank", Seq("customer_id"))

        // Compute line item totals and aggregate per invoice
        val itemTotals = items
            .withColumn("line_total", col("quantity") * col("unit_price"))
            .groupBy("invoice_no")
            .agg(sum("line_total").as("order_total"))

        // Join customer, order, and item tables
        val combined = ordFilled
            .join(custFilled, Seq("customer_id"), "inner")
            .join(itemTotals, Seq("invoice_no"), "inner")

        // Group by country and calculate required aggregations
        combined
            .groupBy("country")
            .agg(
                countDistinct("invoice_no").as("num_orders"),
                avg("order_total").as("avg_order_amount")
            )
            .select("country", "num_orders", "avg_order_amount")
    }

    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        val custSchema = StructType(Array(
            StructField("customer_id", StringType, true),
            StructField("country", StringType, true)
        ))

        val ordSchema = StructType(Array(
            StructField("invoice_no", StringType, true),
            StructField("customer_id", StringType, true)
        ))

        val itemSchema = StructType(Array(
            StructField("invoice_no", StringType, true),
            StructField("quantity", IntegerType, true),
            StructField("unit_price", DoubleType, true)
        ))

        val customers = spark.read.option("header", "true").schema(custSchema).csv("/datasets/orders/customers")
        val orders = spark.read.option("header", "true").schema(ordSchema).csv("/datasets/orders/orders")
        val items = spark.read.option("header", "true").schema(itemSchema).csv("/datasets/orders/items")

        (customers, orders, items)
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._
        val customers = Seq(("C1", "USA"), (null.asInstanceOf[String], "Canada")).toDF("customer_id", "country")
        val orders = Seq(("INV1", "C1"), ("INV2", "blank")).toDF("invoice_no", "customer_id")
        val items = Seq(("INV1", 2, 10.0), ("INV2", 1, 15.0)).toDF("invoice_no", "quantity", "unit_price")

        (customers, orders, items)
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        val (c, o, i) = getTestDF(spark)
        doOrders(c, o, i)
    }

    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)
    }

}
