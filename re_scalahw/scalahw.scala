case class Neumaier(sum: Double, c: Double)

object HW {

    // note it specifies the input (n) and its type (Int) along with the output
    // type List[Int] ( a list of integers)
    def q1(n: Int):List[Int] = {
       List.tabulate(n)(i => (i+1)^2)  
       // returns something of the correct output type in order to compile
    }
    def q2(n: Int):Vector[Double] = {
       Vector.tabulate(n)(i => math.sqrt(i+1))
    
    }
    // In order to get the code to compile, you have to do the same with the rest of the
    // questions, and then fill in code to make them correct.

    def q3(seq: Seq[Double]): Double = {
         seq.foldLeft(0.0)(_ + _)
    
    }

    // you fill in the rest
     def q4(seq: Seq[Double]): Double = {
        seq.foldLeft(1.0)(_ * _)
        }

    def q5(seq: Seq[Double]): Double = {
      seq.foldLeft(0.0)((acc, x) => acc + math.log(x))
      }
    
    def q6(seq: Seq[(Double, Double)]): (Double, Double) = {
        seq.foldLeft((0.0, 0.0)) {
          case ((sum1, sum2), (x, y)) => (sum1 + x, sum2 + y)
        }
      
    }
    def q7(input: Seq[(Double, Double)]): (Double, Double) = {
  input.foldLeft((0.0, Double.NegativeInfinity)) {
    case ((sum, maxVal), (x, y)) => (sum + x, math.max(maxVal, y))
  }
}
    def q8(n: Int): (Int, Int) = {
  (1 to n).foldLeft((0, 1)) {
    case ((sum, prod), x) => (sum + x, prod * x)
  }
}
    def q9(input: Seq[Int]): Int = {
  input
    .filter(_ % 2 == 0)
    .map(x => x * x)
    .foldLeft(0)(_ + _)
}
    def q10(input: Seq[Double]): Double = {
  input.foldLeft((0.0, 1)) {
    case ((sum, idx), value) => (sum + value * idx, idx + 1)
  }._1
}
    def q11(input: Seq[Double]): Double = {
  val result = input.foldLeft(Neumaier(0.0, 0.0)) {
    case (Neumaier(sum, c), x) =>
      val t = sum + x
      val newC =
        if (math.abs(sum) >= math.abs(x))
          c + (sum - t) + x
        else
          c + (x - t) + sum

      Neumaier(t, newC)
  }

  result.sum + result.c
}


}
