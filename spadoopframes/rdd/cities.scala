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

     
        val zipCounterFixed = udf({x: String => Option(x) match {
                                 case Some(y) if y.trim().isEmpty => 0
                                 case Some(y) => y.trim().split("\\s+").length
                                 case None    => 0}
                             })
        spark.udf.register("zipCounterFixed", zipCounterFixed)
    }

    def doCity(input: DataFrame): DataFrame = {
       
        input
            .filter(col("zip").isNotNull)
            .select(callUDF("zipCounterFixed", col("zip")).alias("numZips"))
            .groupBy("numZips")
            .count()
            .withColumnRenamed("count", "numCities")
            .orderBy("numZips")
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
            .option("mode", "PERMISSIVE")  
            .schema(schema)
            .load("/datasets/cities")
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

        val rows = Seq(
            Row("CityA", "PA", "CountyA", 1000,  "",                   1),  
            Row("CityB", "PA", "CountyB", 2000,  "16801",              2),  
            Row("CityC", "PA", "CountyC", 3000,  "16801 16802",        3),  
            Row("CityD", "OH", "CountyD", 4000,  "44101 44102",        4), 
            Row("CityE", "OH", "CountyE", 5000,  "44201 44202 44203",  5),  
            Row("BAD",   null,  null,     null,   null,                -1)   
        )

        spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            schema
        )
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._

        val schema = StructType(Array(
            StructField("numZips",   IntegerType, nullable = false),
            StructField("numCities", LongType,    nullable = false)
        ))

        val rows = Seq(
            Row(0, 1L),
            Row(1, 1L),
            Row(2, 2L),
            Row(3, 1L)
        )

        spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            schema
        ).orderBy("numZips")
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }
}
