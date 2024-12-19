import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object providerReport extends App {
  val spark = SparkSession
    .builder()
    .appName("demo")
    .master("local[*]")
    .getOrCreate()

//  Data Extraction
  val providers_schema = StructType(Array(
    StructField("provider_id", IntegerType, nullable = false),
    StructField("provider_specialty", StringType, nullable = false),
    StructField("first_name", StringType, nullable = false),
    StructField("middle_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = false)
  ))

  val visits_schema = StructType(Array(
    StructField("visit_id", IntegerType, nullable = false),
    StructField("provider_id", IntegerType, nullable = false),
    StructField("visit_date", StringType, nullable = false)
  ))

  val providersDF = spark.read
    .schema(providers_schema)
    .option("header","true")
    .option("delimiter", "|")
    .csv("src/data/providers.csv")

  val visitsDF = spark.read
    .option("inferSchema", "false")
    .schema(visits_schema)
    .csv("src/data/visits.csv")

  // verify the incoming data with Schema provided
  println(providersDF.show(10,false))
  println(visitsDF.show(10))


  //perform transformations

  // Total Visits per Provider
  val totalVisitsPerProvider = visitsDF
    .join(providersDF, Seq("provider_id"), "inner")
    .groupBy("provider_specialty", "provider_id", "first_name", "middle_name", "last_name")
    .agg(count("visit_id").alias("totalVisits"))
    .withColumn("FullName", concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
    .select("provider_id", "FullName", "provider_specialty", "totalVisits")

  // calculate the total number of visits per provider per month.
  val totalVisitsPerProviderPerMonth = visitsDF
    .join(providersDF, Seq("provider_id"), "inner")
    .withColumn("visit_month", date_format(col("visit_date"), "yyyy-MM"))
    .groupBy("provider_id", "visit_month")
    .agg(count("visit_id").alias("totalVisits"))
    .select("provider_id", "visit_month", "totalVisits")


  // Load The Data in JSON FORMAT
  totalVisitsPerProvider
    .write
    .mode("overwrite")
    .partitionBy("provider_specialty")
    .json("output/totalVisitsPerProvider")

  totalVisitsPerProviderPerMonth
    .write
    .mode("overwrite")
    .json("output/totalVisitsPerProviderPerMonth")
}
