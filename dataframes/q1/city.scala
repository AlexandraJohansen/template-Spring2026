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
        // Filter nulls strictly for required columns
        val valid = input.filter(
            col("state").isNotNull && 
            col("county").isNotNull && 
            col("population").isNotNull
        )

        // Annotate small cities (<= 1 zip code) using UDF
        val enriched = valid.withColumn(
            "is_small",
            expr("zipCounter(zips) <= 1").cast("int")
        )

        // Group by State and County first to compute small city count per county
        val countyStats = enriched
            .groupBy("state", "county")
            .agg(
                count("*").as("county_city_count"),
                sum("population").as("county_pop"),
                sum("is_small").as("small_city_count")
            )
            .withColumn("is_le_2_small", expr("int(small_city_count <= 2)"))

        // Aggregate by State to reach target schema
        countyStats
            .groupBy("state")
            .agg(
                sum("county_city_count").as("num_cities"),
                sum("county_pop").as("total_population"),
                sum("is_le_2_small").as("num_counties_le_2_small")
            )
            .select("state", "num_cities", "total_population", "num_counties_le_2_small")
    }

    def getDF(spark: SparkSession): DataFrame = {
        val schema = StructType(Array(
            StructField("name", StringType, true),
            StructField("state", StringType, true),
            StructField("county", StringType, true),
            StructField("population", LongType, true),
            StructField("zips", StringType, true),
            StructField("id", StringType, true)
        ))

        spark.read
            .option("sep", "\t")
            .option("header", "false")
            .schema(schema)
            .csv("/datasets/cities")
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(
            ("Seattle", "WA", "King", 750000L, "98101 98102", "1"),
            ("SmallTown", "WA", "King", 1000L, "98199", "2"),
            ("TinyVille", "WA", "King", 500L, "98198", "3"),
            ("MicroVille", "WA", "King", 200L, "98197", "4")
        ).toDF("name", "state", "county", "population", "zips", "id")
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        doCity(getTestDF(spark))
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }

}
