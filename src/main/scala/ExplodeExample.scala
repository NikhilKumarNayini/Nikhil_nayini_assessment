import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.StdIn

object ExplodeExample extends App {

  val sentence = "Hello this is a sentence"

  val words = sentence.split(" ")

  val first_letters = words.map(word => word.charAt(0))

  val last_letters = words.map(word => word(word.length-1))

  val lenth_of_each_word = words.map(word=> word.length)

  val smallest_word = words.minBy(_.length)
  val maximum_word = words.maxBy(_.length)

  val sentence_reverse = words.reverse.mkString(" ")


  println(first_letters.mkString(" "))
  println(last_letters.mkString(" "))
  println("no of words " + words.length)
  println("length of each word " + lenth_of_each_word.mkString(" "))
  println("smallest word " + smallest_word)
  println("maximum word " + maximum_word)
  println("reverse of sentence : " + sentence_reverse)

}