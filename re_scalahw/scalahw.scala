import scala.math._

case class Neumaier(sum: Double, c: Double)

object HW {

  // Question 1: Returns a List[Int] containing the first n square numbers (1, 4, 9, ...)
  def q1(n: Int): List[Int] = {
    if (n <= 0) List.empty[Int]
    else List.tabulate(n)(i => (i + 1) * (i + 1))
  }

  // Question 2: Returns a Vector[Double] containing square roots of the first n positive numbers (1..n)
  def q2(n: Int): Vector[Double] = {
    if (n <= 0) Vector.empty[Double]
    else Vector.tabulate(n)(i => sqrt((i + 1).toDouble))
  }

  // Question 3: Sum of items in Seq[Double] using foldLeft (safe for empty lists)
  def q3(x: Seq[Double]): Double = {
    x.foldLeft(0.0)(_ + _)
  }

  // Question 4: Product of items in Seq[Double] using foldLeft (safe for empty lists)
  def q4(x: Seq[Double]): Double = {
    x.foldLeft(1.0)(_ * _)
  }

  // Question 5: Sum of natural logs using foldLeft without map
  def q5(x: Seq[Double]): Double = {
    x.foldLeft(0.0)((acc, elem) => acc + log(elem))
  }

  // Question 6: Sum of 1st items and sum of 2nd items in a single foldLeft
  def q6(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, 0.0)) { case ((sum1, sum2), (a, b)) =>
      (sum1 + a, sum2 + b)
    }
  }

  // Question 7: Sum of 1st items and max of 2nd items in a single foldLeft
  def q7(x: Seq[(Double, Double)]): (Double, Double) = {
    x.foldLeft((0.0, Double.NegativeInfinity)) { case ((sum, maxVal), (a, b)) =>
      (sum + a, max(maxVal, b))
    }
  }

  // Question 8: Sum and product of the first n integers using 1 range/tabulate and 1 foldLeft
  def q8(n: Int): (Int, Int) = {
    if (n <= 0) (0, 1)
    else (1 to n).foldLeft((0, 1)) { case ((sum, prod), i) =>
      (sum + i, prod * i)
    }
  }

  // Question 9: Sum of squares of even numbers using 1 filter, 1 map, and 1 reduce/foldLeft
  def q9(x: Seq[Int]): Int = {
    val evens = x.filter(_ % 2 == 0)
    if (evens.isEmpty) 0
    else evens.map(i => i * i).foldLeft(0)(_ + _)
  }

  // Question 10: Compute sum_{i=0}^{n-1} input(i) * (i + 1) using foldLeft without indexing
  def q10(x: Seq[Double]): Double = {
    val (totalSum, _) = x.foldLeft((0.0, 1.0)) { case ((accSum, idx), elem) =>
      (accSum + elem * idx, idx + 1.0)
    }
    totalSum
  }

  // Question 11: Neumaier Summation algorithm using foldLeft and Neumaier case class
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
