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
        // Filter nulls only on columns we need: state, county, population
        // zip can stay null — zipCounter handles None → 0
        val clean = input.filter(
            col("state").isNotNull &&
            col("county").isNotNull &&
            col("population").isNotNull
        )
 
        // Per city: add isSmall flag (1 if zipCounter <= 1, else 0)
        val withSmall = clean.selectExpr(
            "name",
            "state",
            "county",
            "population",
            "int(zipCounter(zip) <= 1) as isSmall"
        )
 
        // Group by (state, county) to get smallCount per county,
        // then flag the county (1 if smallCount <= 2),
        // then group by state to sum: numCities, totalPop, fewSmallCounties
        //
        // We do this in two groupBy steps — NO joins allowed.
        //
        // Step 1: group by (state, county)
        //   - count cities in that county
        //   - sum population in that county
        //   - sum isSmall to get smallCount per county
        val perCounty = withSmall
            .groupBy("state", "county")
            .agg(
                count("name").alias("countyNumCities"),
                sum("population").alias("countyPop"),
                sum("isSmall").alias("smallCount")
            )
            // Flag: 1 if this county has <= 2 small cities
            .selectExpr(
                "state",
                "county",
                "countyNumCities",
                "countyPop",
                "int(smallCount <= 2) as fewSmall"
            )
 
        // Step 2: group by state — sum up city counts, population, qualifying counties
        perCounty
            .groupBy("state")
            .agg(
                sum("countyNumCities").alias("numCities"),
                sum("countyPop").alias("totalPop"),
                sum("fewSmall").alias("fewSmallCounties")
            )
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
 
        // PA / CountyA: CityA(0 zips=small), CityB(1 zip=small), CityC(2 zips=not small)
        //   smallCount=2 → fewSmall=1 (qualifies, ≤2)
        // PA / CountyB: CityD(0=small), CityE(0=small), CityF(0=small)
        //   smallCount=3 → fewSmall=0 (does not qualify)
        // PA total: 6 cities, pop=15000, 1 qualifying county
        //
        // OH / CountyC: CityG(1 zip=small)
        //   smallCount=1 → fewSmall=1 (qualifies)
        // OH total: 1 city, pop=7000, 1 qualifying county
        val rows = Seq(
            Row("CityA", "PA", "CountyA", 1000, "",            1),
            Row("CityB", "PA", "CountyA", 2000, "16801",       2),
            Row("CityC", "PA", "CountyA", 3000, "16801 16802", 3),
            Row("CityD", "PA", "CountyB", 1000, "",            4),
            Row("CityE", "PA", "CountyB", 4000, "",            5),
            Row("CityF", "PA", "CountyB", 4000, "",            6),
            Row("CityG", "OH", "CountyC", 7000, "44101",       7),
            Row("BAD",   null,  null,     null,  null,         -1)
        )
 
        spark.createDataFrame(
            spark.sparkContext.parallelize(rows), schema)
    }
 
    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
 
        val schema = StructType(Array(
            StructField("state",            StringType, nullable = true),
            StructField("numCities",        LongType,   nullable = true),
            StructField("totalPop",         LongType,   nullable = true),
            StructField("fewSmallCounties", LongType,   nullable = true)
        ))
 
        val rows = Seq(
            Row("PA", 6L, 15000L, 1L),
            Row("OH", 1L,  7000L, 1L)
        )
 
        spark.createDataFrame(
            spark.sparkContext.parallelize(rows), schema)
    }
 
    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }
}
