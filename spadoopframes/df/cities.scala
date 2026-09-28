import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {

    def main(args: Array[String]) = {
        val sc = getSC()
        val myrdd = getRDD(sc)
        val counts = doCity(myrdd)
        saveit(counts, "everything_q2")
    }

    def getSC(): SparkContext = {
        val conf = new SparkConf().setAppName("Q2Cities").setIfMissing("spark.master", "local[*]")
        new SparkContext(conf)
    }

    def getRDD(sc: SparkContext): RDD[String] = {
        sc.textFile("/datasets/cities")
    }

    def doCity(input: RDD[String]): RDD[(Int, Int)] = {
    
        input
            .map(line => line.split("\t", -1))
            .filter(fields => fields.length >= 5 && fields(0).trim.nonEmpty)
            .map(fields => {
                val zipField = fields(4).trim
                // Empty zip field → 0 zips; otherwise count whitespace-separated tokens
                val zipCount = if (zipField.isEmpty) 0 else zipField.split("\\s+").length
                (zipCount, 1)   // (numZips, 1) for this city
            })
            .reduceByKey(_ + _)   // (numZips, numCities)
    }

    def getTestRDD(sc: SparkContext): RDD[String] = {
        
        val lines = Seq(
            "CityA\tPA\tCountyA\t1000\t\t1",                    // 0 zips (empty field)
            "CityB\tPA\tCountyB\t2000\t16801\t2",               // 1 zip
            "CityC\tPA\tCountyC\t3000\t16801 16802\t3",         // 2 zips
            "CityD\tOH\tCountyD\t4000\t44101 44102\t4",         // 2 zips
            "CityE\tOH\tCountyE\t5000\t44201 44202 44203\t5",   // 3 zips
            "BADLINE\tonly_two_fields"                           // bad line → filtered
        )
        sc.parallelize(lines)
    }

    def expectedOutput(sc: SparkContext): RDD[(Int, Int)] = {
       
        sc.parallelize(Seq(
            (0, 1),
            (1, 1),
            (2, 2),
            (3, 1)
        ))
    }

    def saveit(myrdd: RDD[(Int, Int)], name: String) = {
        myrdd.saveAsTextFile(name)
    }
}
