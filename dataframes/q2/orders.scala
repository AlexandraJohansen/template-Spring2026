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
        val spark = customers.sparkSession

        // Fill missing customer IDs with "blank" as specified
        val custFilled = customers.na.fill("blank", Seq("customer_id"))
        val ordFilled = orders.na.fill("blank", Seq("customer_id"))

        // Register DataFrames as SQL temporary views
        custFilled.createOrReplaceTempView("customers_view")
        ordFilled.createOrReplaceTempView("orders_view")
        items.createOrReplaceTempView("items_view")

        // Compute total per order, aggregate by country
        spark.sql("""
            WITH order_totals AS (
                SELECT 
                    invoice_no, 
                    SUM(quantity * unit_price) AS order_total
                FROM items_view
                GROUP BY invoice_no
            ),
            customer_orders AS (
                SELECT 
                    c.country,
                    o.invoice_no,
                    t.order_total
                FROM orders_view o
                INNER JOIN customers_view c ON o.customer_id = c.customer_id
                INNER JOIN order_totals t ON o.invoice_no = t.invoice_no
            )
            SELECT 
                country,
                COUNT(DISTINCT invoice_no) AS num_orders,
                AVG(order_total) AS avg_order_amount
            FROM customer_orders
            GROUP BY country
        """)
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
