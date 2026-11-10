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
        val conf = new SparkConf().setAppName("Q2Cities")
        new SparkContext(conf)
    }

    def getRDD(sc: SparkContext): RDD[String] = {
        sc.textFile("/datasets/cities")
    }

    def isIntOrEmpty(x: String): Boolean = {
        x.isEmpty || x.matches("-?[0-9]+")
    }

    def doCity(input: RDD[String]): RDD[(Int, Int)] = {
        // fields: name, state, county, population, zips, id
        // a bad line has the wrong number of fields or a non-numeric population/id
        // (an empty population/id is fine, an empty zips field means 0 zips)
        input
            .map(line => line.split("\t", -1))
            .filter(fields => fields.length == 6 &&
                              isIntOrEmpty(fields(3).trim) &&
                              isIntOrEmpty(fields(5).trim))
            .map(fields => {
                val zips = fields(4).trim
                val zipCount = if (zips.isEmpty) 0 else zips.split("\\s+").length
                (zipCount, 1)
            })
            .reduceByKey(_ + _)
    }

    def getTestRDD(sc: SparkContext): RDD[String] = {
        val lines = Seq(
            "CityA\tPA\tCountyA\t1000\t\t1",
            "CityB\tPA\tCountyB\t2000\t16801\t2",
            "CityC\tPA\tCountyC\t3000\t16801 16802\t3",
            "CityD\tOH\tCountyD\t4000\t44101 44102\t4",
            "CityE\tOH\tCountyE\t5000\t44201 44202 44203\t5",
            "CityF\tOH\tCountyF\t\t\t6",
            "BADLINE\tonly_two_fields",
            "BAD2\tPA\tCountyZ\t10\t16801",
            "BAD3\tPA\tCountyZ\tabc\t16801\t9"
        )
        sc.parallelize(lines)
    }

    def expectedOutput(sc: SparkContext): RDD[(Int, Int)] = {
        sc.parallelize(Seq(
            (0, 2),
            (1, 1),
            (2, 2),
            (3, 1)
        ))
    }

    def saveit(myrdd: RDD[(Int, Int)], name: String) = {
        myrdd.saveAsTextFile(name)
    }
}
