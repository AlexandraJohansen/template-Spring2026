object Tester extends App {
    
    val q1answer: List[Int] = HW.q1(3)
    println(s"q1: $q1answer")

    val q2answer: Vector[Double] = HW.q2(4)
    println(s"q2: $q2answer")

    val q3answer_a: Double = HW.q3(List(1.0, 2.0, 3.0))
    val q3answer_b: Double = HW.q3(Vector(3.0, 4.0, 5.0))
    println(s"q3_a: $q3answer_a | q3_b: $q3answer_b")

    val q4answer_a: Double = HW.q4(List(1.0, 2.0, 3.0))
    val q4answer_b: Double = HW.q4(Vector(3.0, 4.0, 5.0))
    println(s"q4_a: $q4answer_a | q4_b: $q4answer_b")

    val q5answer_a: Double = HW.q5(List(1.0, 2.0, 3.0))
    val q5answer_b: Double = HW.q5(Vector(3.0, 4.0, 5.0))
    println(s"q5_a: $q5answer_a | q5_b: $q5answer_b")

    val q6answer_a: (Double, Double) = HW.q6(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    val q6answer_b: (Double, Double) = HW.q6(Vector((3.0, 4.0), (5.0, 6.0)))
    println(s"q6_a: $q6answer_a | q6_b: $q6answer_b")

    val q7answer_a: (Double, Double) = HW.q7(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    val q7answer_b: (Double, Double) = HW.q7(Vector((3.0, 4.0), (5.0, 6.0)))
    println(s"q7_a: $q7answer_a | q7_b: $q7answer_b")

    val q8answer: (Int, Int) = HW.q8(4)
    println(s"q8: $q8answer")

    val q9answer_a: Int = HW.q9(List(4, 5, 8, 7))
    val q9answer_b: Int = HW.q9(Vector(4, 5, 8, 7))
    println(s"q9_a: $q9answer_a | q9_b: $q9answer_b")

    val q10answer_a: Double = HW.q10(List(4.0, 5.0, 8.0, 7.0))
    val q10answer_b: Double = HW.q10(Vector(4.0, 5.0, 8.0, 7.0))
    println(s"q10_a: $q10answer_a | q10_b: $q10answer_b")

    val q11answer_a: Double = HW.q11(List(1.0, 1e100, 1.0, -1e100))
    val q11answer_b: Double = HW.q11(Vector(1.0, 1e100, 1.0, -1e100))
    println(s"q11_a: $q11answer_a | q11_b: $q11answer_b")
}
