import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object practice {
  def AverageSalaryByDepartment(spark: SparkSession):sql.DataFrame = {
    val data = Seq(
      (1,"John", "HR", 5500),
      (2,"Doe", "IT", 6500),
      (3,"Jane", "HR", 4000),
      (4,"Smith", "IT", 7500),
      (5,"Emily", "Finance", 5000),
      (6,"Chris", "Finance", 5500),
      (1,"John", "HR", 5000),
      (2,"Doe", "IT", 6000),
      (3,"Jane", "HR", 4500),
      (4,"Smith", "IT", 7000),
      (5,"Emily", "Finance", 5500),
      (6,"Chris", "Finance", 5000)
    )
    val columns = Seq("id","name", "department", "salary")
    import spark.implicits._
    val df = data
      .toDF(columns: _*)
    println("Original Dataframe")
    df.show()

    val deduplicated_df = df.dropDuplicates("id")

    println("Deduplicated Dataframe")
    deduplicated_df.show()

    val avg_salary_dept =
      deduplicated_df.groupBy("department")
      .avg("salary")
      .withColumnRenamed("avg(salary)", "average_salary")

    avg_salary_dept
  }
}