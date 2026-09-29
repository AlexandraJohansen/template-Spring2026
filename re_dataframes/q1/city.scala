import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
 
object Q1 {
 
    def main(args: Array[String]) = {
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val counts = doCity(mydf)
        saveit(counts, "dataframes_q1")
    }
 
    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {
                                 case Some(y) => (y.trim()+" ").split("\\s+").size;
                                 case None => 0}
                             })
        spark.udf.register("zipCounter", zipCounter)
    }
 
    def doCity(input: DataFrame): DataFrame = {
        // Step 1: filter nulls only on columns we need: state, county, population, zip
        val clean = input.filter(
            col("state").isNotNull &&
            col("county").isNotNull &&
            col("population").isNotNull
            // zip can be null — zipCounter handles None → 0
        )
 
        // Step 2: per city, determine if it is small (zipCounter <= 1)
        // A county is uniquely identified by (state, county)
        // isSmall is 1 if small city, 0 otherwise — use int() casting in SQL expr
        val withSmall = clean.selectExpr(
            "name",
            "state",
            "county",
            "population",
            "int(zipCounter(zip) <= 1) as isSmall"
        )
 
        // Step 3: aggregate per (state, county) to count small cities in that county
        val perCounty = withSmall
            .groupBy("state", "county")
            .agg(sum("isSmall").alias("smallCount"))
 
        // Step 4: flag each county as having 2 or fewer small cities
        // int(smallCount <= 2) → 1 if county qualifies, 0 otherwise
        val countyFlagged = perCounty.selectExpr(
            "state",
            "county",
            "int(smallCount <= 2) as fewSmall"
        )
 
        // Step 5: aggregate per state:
        //   - number of cities (from clean, before county grouping)
        //   - total population
        //   - number of counties with fewSmall = 1
        val cityStats = clean
            .groupBy("state")
            .agg(
                count("name").alias("numCities"),
                sum("population").alias("totalPop")
            )
 
        val countyStats = countyFlagged
            .groupBy("state")
            .agg(sum("fewSmall").alias("fewSmallCounties"))
 
        // Step 6: join city stats and county stats on state
        // Use withColumnRenamed to avoid ambiguous column names
        cityStats
            .join(countyStats.withColumnRenamed("state", "state2"),
                  col("state") === col("state2"))
            .select("state", "numCities", "totalPop", "fewSmallCounties")
    }
 
    def getDF(spark: SparkSession): DataFrame = {
        val schema = StructType(Array(
            StructField("name",       StringType,  nullable = true),
            StructField("state",      StringType,  nullable = true),
            StructField("county",     StringType,  nullable = true),
            StructField("population", IntegerType, nullable = true),
            StructField("zip",        StringType,  nullable = true),
            StructField("id",         IntegerType, nullable = true)
        ))
 
        spark.read
            .format("csv")
            .option("sep", "\t")
            .option("header", "false")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .load("/datasets/cities")
    }
 
    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark)
        spark
    }
 
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._
 
        val schema = StructType(Array(
            StructField("name",       StringType,  nullable = true),
            StructField("state",      StringType,  nullable = true),
            StructField("county",     StringType,  nullable = true),
            StructField("population", IntegerType, nullable = true),
            StructField("zip",        StringType,  nullable = true),
            StructField("id",         IntegerType, nullable = true)
        ))
 
        // PA / CountyA: CityA (0 zips=small), CityB (1 zip=small), CityC (2 zips=not small)
        //   → 3 cities, pop=6000, CountyA has 2 small cities (≤2 → qualifies)
        // PA / CountyB: CityD (0 zips=small), CityE (0 zips=small), CityF (0 zips=small)
        //   → 3 cities, pop=9000, CountyB has 3 small cities (>2 → does not qualify)
        // PA total: 6 cities, pop=15000, 1 county with ≤2 small cities (CountyA only)
        //
        // OH / CountyC: CityG (1 zip=small)
        //   → 1 city, pop=7000, CountyC has 1 small city (≤2 → qualifies)
        // OH total: 1 city, pop=7000, 1 county with ≤2 small cities
        val rows = Seq(
            Row("CityA", "PA", "CountyA", 1000, "",              1),
            Row("CityB", "PA", "CountyA", 2000, "16801",         2),
            Row("CityC", "PA", "CountyA", 3000, "16801 16802",   3),
            Row("CityD", "PA", "CountyB", 1000, "",              4),
            Row("CityE", "PA", "CountyB", 4000, "",              5),
            Row("CityF", "PA", "CountyB", 4000, "",              6),
            Row("CityG", "OH", "CountyC", 7000, "44101",         7),
            Row("BAD",   null,  null,     null,  null,           -1)
        )
 
        spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            schema
        )
    }
 
    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
 
        val schema = StructType(Array(
            StructField("state",            StringType, nullable = true),
            StructField("numCities",        LongType,   nullable = false),
            StructField("totalPop",         LongType,   nullable = true),
            StructField("fewSmallCounties", LongType,   nullable = true)
        ))
 
        val rows = Seq(
            Row("PA", 6L, 15000L, 1L),
            Row("OH", 1L,  7000L, 1L)
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
 
