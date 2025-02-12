import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object hello {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    // Set the log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

//    practice.AverageSalaryByDepartment(spark).show()

//    capGeminiTask.capGeminiTask(spark).show()

//    capGeminiTask.dataCleaning(spark).show(truncate = false)

    capGeminiTask.userDefinedFunctionExample(spark).show()


//    // providerReport
//    providerReport.extract(spark)
//    providerReport.transform(
//      providerReport.extract(spark)._1,
//      providerReport.extract(spark)._2 )
//    providerReport.load(
//      providerReport.transform( providerReport.extract(spark)._1, providerReport.extract(spark)._2)._1,
//      providerReport.transform( providerReport.extract(spark)._1, providerReport.extract(spark)._2)._2 )
//
//    // Stop the spark session
//    spark.stop()
//
//   // functions
//    println("Enter a number to check the sum of numbers:")
//    println(functions.sum_of_digits(StdIn.readInt()))
//
//    println("Enter a number to check if it is prime:")
//    println(functions.isPrime(StdIn.readInt()))
//
//    println("Enter a number to find the factorial:")
//    println("factorial of the number = " + functions.factorial(BigInt(StdIn.readLine())))
//
//    println("Enter a list of integers separated by space:")
//    val collection = StdIn.readLine().split(" ").map(BigInt(_)).toList
//    println("input collection " + collection)
//    println("quadral function on a collection =  " + collection.map(functions.quadral))
//    println("mapping with *3 = " + collection.map(_ * 3))
//
//    println("Enter a positive integer N:")
//    println("sum of squares of the integers from to 1 to " + functions.sum_of_squares((StdIn.readInt())))
//
//    println("Enter a positive integer A:")
//    println("sum of the integers from to 1 to = " + functions.sum_of_integers(StdIn.readInt()))
  }
}
