import scala.math._

case class Neumaier(sum: Double, c: Double)

object HW {

  def q1(n: Int): List[Int] = {
    List.tabulate(n)(i => (i + 1) * (i + 1))
  }


  def q2(n: Int): Vector[Double] = {
    Vector.tabulate(n)(i => sqrt(i + 1))
  }


  def q3(x: Seq[Double]): Double = {
    x.foldLeft(0.0)(_ + _)
  }
  def q4(x: Seq[Double]): Double = {
    x.foldLeft(1.0)(_ * _)
  }


  def q5(x: Seq[Double]): Double = {
    x.foldLeft(0.0)((acc, elem) => acc + log(elem))
  }

 
  def q6(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, 0.0)) { case ((sum1, sum2), (a, b)) =>
      (sum1 + a, sum2 + b)
    }
  }

  def q7(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, Double.NegativeInfinity)) { case ((sum, maxVal), (a, b)) =>
      (sum + a, max(maxVal, b))
    }
  }


  def q8(n: Int): (Long, Long) = {
    (1 to n).foldLeft((0L, 1L)) { case ((sum, prod), i) =>
      (sum + i, prod * i)
    }
  }


  def q9(x: Seq[Int]): Int = {
    val evens = x.filter(_ % 2 == 0)
    if (evens.isEmpty) 0
    else evens.map(i => i * i).reduce(_ + _)
  }


  def q10(x: Seq[Double]): Double = {
    val (totalSum, _) = x.foldLeft((0.0, 1.0)) { case ((accSum, idx), elem) =>
      (accSum + elem * idx, idx + 1.0)
    }
    totalSum
  }
    def q11(x: Seq[Double]): Double = {
    val finalState = x.foldLeft(Neumaier(0.0, 0.0)) { (state, elem) =>
      val t = state.sum + elem
      val deltaC = if (abs(state.sum) >= abs(elem)) {
        (state.sum - t) + elem
      } else {
        (elem - t) + state.sum
      }
      Neumaier(t, state.c + deltaC)
    }
    finalState.sum + finalState.c
  }

}
