import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3 {

    def main(args: Array[String]) = {
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doCity(mydf)
        saveit(answer, "everything_q3")
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {
                                 case Some(y) => (y.trim()+" ").split("\\s+").size;
                                 case None => 0}
                             })
        spark.udf.register("zipCounter", zipCounter)
    }

    def doCity(input: DataFrame): DataFrame = {
        // Bad lines were already removed in getDF.
        // zip must NOT be filtered: an empty zip field is read as null and means 0 zips
        // (zipCounter already maps null to 0).
        input
            .selectExpr("zipCounter(zip) as numZips")
            .groupBy("numZips")
            .count()
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark)
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        // The extra "corrupt" column receives the raw text of any line spark could not parse
        // (wrong number of fields, or a value that does not fit its type). Those are the bad lines.
        val schema = StructType(Array(
            StructField("name",       StringType,  nullable = true),
            StructField("state",      StringType,  nullable = true),
            StructField("county",     StringType,  nullable = true),
            StructField("population", IntegerType, nullable = true),
            StructField("zip",        StringType,  nullable = true),
            StructField("id",         IntegerType, nullable = true),
            StructField("corrupt",    StringType,  nullable = true)
        ))

        val raw = spark.read
            .format("csv")
            .option("sep", "\t")
            .option("header", "false")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "corrupt")
            .schema(schema)
            .load("/datasets/cities")

        // cache() makes spark parse every column, so the corrupt flag is reliable
        // even when a later query only uses the zip column
        raw.cache()
            .filter(col("corrupt").isNull)
            .drop("corrupt")
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._

        // None becomes null in the dataframe
        Seq[(String, String, String, Int, Option[String], Option[Int])](
            ("CityA", "PA", "CountyA", 1000, Some(""),                   Some(1)),  // 0 zips (empty)
            ("CityB", "PA", "CountyB", 2000, Some("16801"),              Some(2)),  // 1 zip
            ("CityC", "PA", "CountyC", 3000, Some("16801 16802"),        Some(3)),  // 2 zips
            ("CityD", "OH", "CountyD", 4000, Some("44101 44102"),        Some(4)),  // 2 zips
            ("CityE", "OH", "CountyE", 5000, Some("44201 44202 44203"),  Some(5)),  // 3 zips
            ("CityF", "OH", "CountyF", 6000, None,                       Some(6))   // null zip = 0 zips
        ).toDF("name", "state", "county", "population", "zip", "id")
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
        // 0 zips: CityA, CityF   1 zip: CityB   2 zips: CityC, CityD   3 zips: CityE
        Seq(
            (0, 2L),
            (1, 1L),
            (2, 2L),
            (3, 1L)
        ).toDF("numZips", "numCities")
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }
}
