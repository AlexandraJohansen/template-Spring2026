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
     
        input
            .filter(col("id").isNotNull)
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
            .schema(schema)
            .load("/datasets/cities")
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._

        Seq(
            ("CityA", "PA", "CountyA", 1000, "",                  1),   // 0 zips (empty)
            ("CityB", "PA", "CountyB", 2000, "16801",             2),   // 1 zip
            ("CityC", "PA", "CountyC", 3000, "16801 16802",       3),   // 2 zips
            ("CityD", "OH", "CountyD", 4000, "44101 44102",       4),   // 2 zips
            ("CityE", "OH", "CountyE", 5000, "44201 44202 44203", 5)    // 3 zips
        ).toDF("name", "state", "county", "population", "zip", "id")
         .union(
       
            Seq(
                ("CityF", "OH", "CountyF", 6000, null.asInstanceOf[String], 6),
                ("BAD",   "PA", "CountyZ", 10,   "16801",                   null.asInstanceOf[Integer])
            ).toDF("name", "state", "county", "population", "zip", "id")
         )
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._
     
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
