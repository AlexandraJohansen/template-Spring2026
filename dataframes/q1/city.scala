import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q1 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark) 
        val counts = doCity(mydf) 
        saveit(counts, "dataframes_q1")  // save the rdd to your home directory in HDFS
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {
                                 case Some(y) => (y.trim()+" ").split("\\s+").size; 
                                 case None => 0}
                             })
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

        def doCity(input: DataFrame): DataFrame = {
  // Step 1: Filter nulls strictly necessary for computation
  val validCities = input.filter(
    col("state").isNotNull && 
    col("county").isNotNull && 
    col("population").isNotNull
  )

  // Step 2: Annotate small cities (1 or fewer zip codes) using registered UDF
  val enriched = validCities.withColumn(
    "is_small",
    expr("zipCounter(zips) <= 1").cast("int")
  )

  // Step 3: Aggregate by State + County first to get county-level small city counts
  val countyStats = enriched
    .groupBy("state", "county")
    .agg(
      count("*").as("county_city_count"),
      sum("population").as("county_pop"),
      sum("is_small").as("small_city_count")
    )
    .withColumn("is_qualifying_county", expr("int(small_city_count <= 2)"))

  // Step 4: Aggregate by State to reach final output schema
  countyStats
    .groupBy("state")
    .agg(
      sum("county_city_count").as("num_cities"),
      sum("county_pop").as("total_population"),
      sum("is_qualifying_county").as("num_counties_le_2_small")
    )
    .select("state", "num_cities", "total_population", "num_counties_le_2_small")
}


    def getDF(spark: SparkSession): DataFrame = {
  val schema = StructType(Array(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("county", StringType, nullable = true),
    StructField("population", LongType, nullable = true),
    StructField("zips", StringType, nullable = true)
  ))

  spark.read
    .option("header", "false")
    .option("sep", "\t")
    .schema(schema)
    .csv("/datasets/cities")
}

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        // check slides carefully. note that you don't need to add headers, unlike RDDs
  Seq(
    (1, "Seattle", "WA", "King", 750000L, "98101 98102"),
    (2, "SmallTown", "WA", "King", 1000L, "98199"),
    (3, "TinyVille", "WA", "King", 500L, "98198"),
    (4, "MicroVille", "WA", "King", 200L, "98197")
  ).toDF("id", "name", "state", "county", "population", "zips")
}

def expectedOutput(spark: SparkSession): DataFrame = {
  doCity(getTestDF(spark))
    }

    

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }

}
