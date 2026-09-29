import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q2 {
 
    def main(args: Array[String]) = {
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c, o, i)
        saveit(counts, "dataframes_q2")
    }
 
    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        // customers: CustomerID, CustomerName, ContactName, Country  (fill null CustomerID → "blank")
        // orders:    OrderID, CustomerID, ...
        // items:     OrderID, ProductID, Quantity, UnitPrice, ...
        //
        // Goal: per country, number of distinct orders and average spend per order
        // Spend per order = sum(Quantity * UnitPrice) for all items in that order
 
        // Step 1: fill null CustomerID in customers with "blank"
        val custFilled = customers.na.fill("blank", Seq("CustomerID"))
 
        // Step 2: compute spend per order from items
        val orderSpend = items
            .selectExpr("OrderID", "Quantity * UnitPrice as spend")
            .groupBy("OrderID")
            .agg(sum("spend").alias("orderTotal"))
 
        // Step 3: join orders → customers to get country per order
        // Rename CustomerID in orders to avoid ambiguity after join
        val ordersRenamed = orders.withColumnRenamed("CustomerID", "ord_CustomerID")
        val custRenamed   = custFilled.withColumnRenamed("CustomerID", "cust_CustomerID")
 
        val ordersWithCountry = ordersRenamed
            .join(custRenamed,
                  col("ord_CustomerID") === col("cust_CustomerID"))
            .select(col("OrderID"), col("Country"))
 
        // Step 4: join with order spend
        val orderSpendRenamed = orderSpend.withColumnRenamed("OrderID", "spend_OrderID")
        val full = ordersWithCountry
            .join(orderSpendRenamed,
                  col("OrderID") === col("spend_OrderID"))
            .select(col("Country"), col("OrderID"), col("orderTotal"))
 
        // Step 5: per country, count orders and average spend per order
        full
            .groupBy("Country")
            .agg(
                countDistinct("OrderID").alias("numOrders"),
                avg("orderTotal").alias("avgSpend")
            )
    }
 
    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // /datasets/orders contains: customers.csv, orders.csv, order_details.csv
        val customersSchema = StructType(Array(
            StructField("CustomerID",   StringType,  nullable = true),
            StructField("CustomerName", StringType,  nullable = true),
            StructField("ContactName",  StringType,  nullable = true),
            StructField("Address",      StringType,  nullable = true),
            StructField("City",         StringType,  nullable = true),
            StructField("PostalCode",   StringType,  nullable = true),
            StructField("Country",      StringType,  nullable = true)
        ))
 
        val ordersSchema = StructType(Array(
            StructField("OrderID",      StringType,  nullable = true),
            StructField("CustomerID",   StringType,  nullable = true),
            StructField("EmployeeID",   StringType,  nullable = true),
            StructField("OrderDate",    StringType,  nullable = true),
            StructField("ShipperID",    StringType,  nullable = true)
        ))
 
        val itemsSchema = StructType(Array(
            StructField("OrderDetailID", StringType,  nullable = true),
            StructField("OrderID",       StringType,  nullable = true),
            StructField("ProductID",     StringType,  nullable = true),
            StructField("Quantity",      IntegerType, nullable = true),
            StructField("UnitPrice",     DoubleType,  nullable = true)
        ))
 
        val customers = spark.read.format("csv")
            .option("sep", ",").option("header", "true")
            .option("mode", "PERMISSIVE").schema(customersSchema)
            .load("/datasets/orders/customers.csv")
 
        val orders = spark.read.format("csv")
            .option("sep", ",").option("header", "true")
            .option("mode", "PERMISSIVE").schema(ordersSchema)
            .load("/datasets/orders/orders.csv")
 
        val items = spark.read.format("csv")
            .option("sep", ",").option("header", "true")
            .option("mode", "PERMISSIVE").schema(itemsSchema)
            .load("/datasets/orders/order_details.csv")
 
        (customers, orders, items)
    }
 
    def getSparkSession(): SparkSession = {
        SparkSession.builder().getOrCreate()
    }
 
    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._
 
        val customersSchema = StructType(Array(
            StructField("CustomerID",   StringType, nullable = true),
            StructField("CustomerName", StringType, nullable = true),
            StructField("ContactName",  StringType, nullable = true),
            StructField("Address",      StringType, nullable = true),
            StructField("City",         StringType, nullable = true),
            StructField("PostalCode",   StringType, nullable = true),
            StructField("Country",      StringType, nullable = true)
        ))
 
        val ordersSchema = StructType(Array(
            StructField("OrderID",    StringType, nullable = true),
            StructField("CustomerID", StringType, nullable = true),
            StructField("EmployeeID", StringType, nullable = true),
            StructField("OrderDate",  StringType, nullable = true),
            StructField("ShipperID",  StringType, nullable = true)
        ))
 
        val itemsSchema = StructType(Array(
            StructField("OrderDetailID", StringType,  nullable = true),
            StructField("OrderID",       StringType,  nullable = true),
            StructField("ProductID",     StringType,  nullable = true),
            StructField("Quantity",      IntegerType, nullable = true),
            StructField("UnitPrice",     DoubleType,  nullable = true)
        ))
 
        // Customers: C1→UK, C2→France, null→Germany (filled to "blank"→Germany)
        val custRows = Seq(
            Row("C1",  "Alice", "Alice A", "1 St", "London",  "E1", "UK"),
            Row("C2",  "Bob",   "Bob B",   "2 Av", "Paris",   "75", "France"),
            Row(null,  "Carol", "Carol C", "3 Rd", "Berlin",  "10", "Germany")
        )
        // Orders: O1→C1(UK), O2→C1(UK), O3→C2(France), O4→blank(Germany)
        val ordRows = Seq(
            Row("O1", "C1",    "E1", "2024-01-01", "S1"),
            Row("O2", "C1",    "E1", "2024-01-02", "S1"),
            Row("O3", "C2",    "E2", "2024-01-03", "S2"),
            Row("O4", "blank", "E3", "2024-01-04", "S3")
        )
        // Items: O1→2*10=20, O2→3*5=15, O3→1*20=20, O4→4*2=8
        val itemRows = Seq(
            Row("D1", "O1", "P1", 2,  10.0),
            Row("D2", "O2", "P2", 3,   5.0),
            Row("D3", "O3", "P3", 1,  20.0),
            Row("D4", "O4", "P4", 4,   2.0)
        )
 
        val customers = spark.createDataFrame(
            spark.sparkContext.parallelize(custRows), customersSchema)
        val orders = spark.createDataFrame(
            spark.sparkContext.parallelize(ordRows), ordersSchema)
        val items = spark.createDataFrame(
            spark.sparkContext.parallelize(itemRows), itemsSchema)
 
        (customers, orders, items)
    }
 
    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
 
        // UK:      2 orders (O1=20, O2=15), avg = (20+15)/2 = 17.5
        // France:  1 order  (O3=20), avg = 20.0
        // Germany: 1 order  (O4=8),  avg = 8.0
        val schema = StructType(Array(
            StructField("Country",   StringType,  nullable = true),
            StructField("numOrders", LongType,    nullable = false),
            StructField("avgSpend",  DoubleType,  nullable = true)
        ))
 
        val rows = Seq(
            Row("UK",      2L, 17.5),
            Row("France",  1L, 20.0),
            Row("Germany", 1L,  8.0)
        )
 
        spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            schema
        )
    }
 
    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }
}
 
