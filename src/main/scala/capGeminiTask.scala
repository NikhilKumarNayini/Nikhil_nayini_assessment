import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object capGeminiTask {
  // You are working as a Data Engineer, and the company has a log system where timestamps are recorded for every user action
  // (e.g., when the user logs in and logs out).
  // Your manager wants to know how much time each user spends between log in and log out.
  // The system generates logs with login_timestamp and logout_timestamp columns.
  // You need to calculate the difference between the logout_timestamp and login_timestamp in hours, minutes, and seconds.
  // The result should be formatted like "HH:mm:ss".
  def capGeminiTask(spark: SparkSession): sql.DataFrame  = {
    spark.sparkContext.setLogLevel("ERROR")
    val data = Seq(
      (1, "2025-01-31 08:00:00", "2025-01-31 10:30:45"),
      (2, "2025-01-31 09:00:30", "2025-01-31 12:15:10"),
      (3, "2025-01-31 07:45:00", "2025-01-31 09:00:15")
    )
    val raw_df = spark.createDataFrame(data).toDF("user_id", "login_timestamp", "logout_timestamp")
    // Convert the login_timestamp and logout_timestamp columns to timestamp
    val df = raw_df
      .withColumn("login_timestamp", unix_timestamp(col("login_timestamp")))
      .withColumn("logout_timestamp", unix_timestamp(col("logout_timestamp")))
      .withColumn("logged_hours",
        format_string(
          "%02d:%02d:%02d",
          ((col("logout_timestamp") - col("login_timestamp")) / 3600).cast("int"),
          (((col("logout_timestamp") - col("login_timestamp")) % 3600) / 60).cast("int"),
          ((col("logout_timestamp") - col("login_timestamp")) % 60).cast("int")
        )
      )
    val final_df = df.select("user_id", "login_timestamp", "logout_timestamp", "logged_hours")

    final_df
  }
  //  Write a Spark program in Scala that takes a DataFrame with bill IDs and order names (as lists of items) and
  //  returns the count of each distinct item across all orders.
  def bills(spark: SparkSession): sql.DataFrame = {

    val data = Seq(
      (101, "[pizza,samosa,idli]"),
      (102, "[kachori,sambhar,idli]"),
      (103, "[dosa,vada,pizza]"),
      (104, "[samosa,idli,chai]"),
      (105, "[pizza,chai,dosa]")
    )

    val raw_df = spark.createDataFrame(data).toDF("billid", "Ordername")

    val dfCleaned = raw_df.withColumn("Ordername", regexp_replace(col("Ordername"), "\\[|\\]", ""))
      .withColumn("Ordername", split(col("Ordername"), ","))
      .withColumn("Item", explode(col("Ordername")))

    val final_df = dfCleaned.select("Item").groupBy("Item").count()
    final_df
  }
  //  Write a Spark program in Scala with columns id, name, and salary,
  //  where the data might contain special characters, blanks, and "NA" values that should be treated as nulls.
  //  Handle the uncertainty of column names being dynamic and uncertain due to potential special characters.
  def dataCleaning(spark: SparkSession): sql.DataFrame = {
    val data = Seq(
      (1, "#", ""),
      (2, "dd", "$"),
      (3, "NA", "5000"),
      (4, "John", "NA"),
      (5, "", "6000")
    )

    val raw_data = spark.createDataFrame(data).toDF("id","name","Salary")
    val specialChars = List("#", "$", "@", "&", "%")

    val cleanedDf =
      raw_data.columns.foldLeft(raw_data)((currentDf, colName) => {
      currentDf.withColumn(colName,
        when(
          col(colName).isin(specialChars: _*) || col(colName).isNull || col(colName).equalTo("NA") || col(colName).equalTo(""),
          lit(null)
        ).otherwise(col(colName))
      )
    })
    cleanedDf
  }
  //   User-Defined Functions (UDFs) in Apache Spark allow you to define custom functions to apply transformations on DataFrame columns.
//  UDFs can be used to perform operations that are not available in built-in Spark SQL functions.
//  Here’s a detailed explanation along with an example.
//  Creating a UDF in Spark
//  To create a UDF in Spark, follow these steps:
//  Define a regular function
//  Register the function as a UDF.
//  Apply the UDF to DataFrame columns.
//  Example
//  Let’s create a simple example where we have a DataFrame containing names, and we want to create a UDF to capitalize the first letter of each name.

  def capitalizeFirstLetter(name: String): String = {
    if (name == null || name.isEmpty) {
      name
    } else {
      name.substring(0, 1).toUpperCase + name.substring(1).toLowerCase
    }
  }
  val capitalizeUDF = udf(capitalizeFirstLetter _)

  def userDefinedFunctionExample (spark: SparkSession): sql.DataFrame = {
    import spark.implicits._
    val df = Seq(
      "rahul",
      "umesh",
      "vijay",
      "pawan"
    ).toDF("name")

    val dfWithCapitalizedNames = df.withColumn("capitalized_name", capitalizeUDF($"name"))
    dfWithCapitalizedNames.show(truncate = false)
    dfWithCapitalizedNames
  }

  def userDefinedFunctionSQLExample (spark: SparkSession): sql.DataFrame = {
    import spark.implicits._
    spark.udf.register("capitalizeFirstLetter", capitalizeFirstLetter(_: String): String)
    val df = Seq(
      "rahul",
      "umesh",
      "vijay",
      "pawan"
    ).toDF("name")

    df.createOrReplaceTempView("names")
    val result = spark.sql("SELECT name, capitalizeFirstLetter(name) AS capitalized_name FROM names")
    result
  }
}
