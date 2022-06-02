import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object Main extends App {
  // Context
  lazy implicit val spark =
    SparkSession.builder()
      .master("local")
      .appName("spark_test")
      .getOrCreate()

  import spark.implicits._ // Required to call the .toDF function later
  val html = scala.io.Source.fromURL("http://files.grouplens.org/datasets/movielens/ml-100k/u.data").mkString // Get all rows as one string
  val seqOfRecords = html.split("\n") // Split based on the newline characters
    .filter(_ != "") // Filter out any empty lines
    .toSeq // Convert to Seq so we can convert to DF later
    .map(row => row.split("\t")) // Split each line on tab character to make an Array of 4 String each
    .map { case Array(f1,f2,f3,f4) => (f1,f2,f3,f4) } // Convert that Array[String] into Array[(String, String, String, String)]

  val df = seqOfRecords.toDF("userID", "movieID", "ratings", "timestamp") // Give whatever column names you want
  // Data manipulation (I do not care where this will be executre)
  df
    .select("ratings") // select the "ratings row"
    .groupBy("ratings") // group by on ratings
    .count // count the total number of row for each rating
    .sort(col("count").desc) // sort the ratings using the count
    .show()

}
