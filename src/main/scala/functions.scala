object functions {

  // below are the curry functions
  // syntax : def curryfunction_name(argument1, argument2) = operation
  def quadral(a: BigInt): BigInt = a * a * a * a
  def factorial(a: BigInt): BigInt = if (a == 0) 1 else a * factorial(a - 1)
  // function in scala that accepts a positive integer N and returns the sum of squares of all the integers from to 1 to N.
  def sum_of_squares(N: Int): Int = (1 to N).map(i => i * i).sum
  // function in scala that accepts a positive integer N and returns the sum of all of the integers from to 1 to N.
  def sum_of_integers(A: Int): Int = (1 to A).sum
  // function to check if a number is prime
  def isPrime(n:Int):Boolean = if (n <= 1) false else !(2 to n / 2).exists(x => n % x == 0)
  // sum of digits of a number
  def sum_of_digits(n: Int): Int = n.toString.map(_.asDigit).sum


}
