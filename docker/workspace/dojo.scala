import org.apache.spark.sql.{SparkSession, functions => F}

// import org.apache.log4j.{Level, Logger}
// Logger.getLogger("org").setLevel(Level.OFF)

// import $ivy.`org.apache.spark::spark-sql:3.1.1`
// import org.apache.spark.sql._

// val spark = {
//   NotebookSparkSession.builder()
//     .master("local[*]")
//     .getOrCreate()
// }

// def sc = spark.sparkContext

// import $ivy.`org.apache.spark::spark-mllib:3.1.1`
// import org.apache.spark.mllib.linalg.{Matrix, Matrices}

// Matrices.dense(3, 2, Array(1, 3, 5, 2, 4, 6))

object BasicDataProcessing {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("BasicDataProcessingExample")
      .master("local[*]")  // Use local[*] for local testing with all available cores
      .getOrCreate()

    // Set the log level to only print errors
    spark.sparkContext.setLogLevel("ERROR")

    // Read input CSV file into a DataFrame
    val inputFilePath = "path/to/your/input/emp.csv"
    val df = spark.read
      .option("header", "true")  // Specifies that the first row contains column headers
      .option("inferSchema", "true")  // Automatically infers data types
      .csv(inputFilePath)

    // Display the schema of the DataFrame
    df.printSchema()

    // Show the first 10 rows of the DataFrame
    df.show(10)

    // Filter rows where a specific column's value is greater than a threshold
    val filteredDF = df.filter(F.col("columnName") > 100)

    // Group by a specific column and count the number of occurrences
    val groupedDF = filteredDF.groupBy("groupColumn").count()

    // Write the result to an output directory in CSV format
    val outputFilePath = "path/to/your/output/folder"
    groupedDF.write
      .option("header", "true")  // Include the header in the output file
      .csv(outputFilePath)

    // Stop the SparkSession
    spark.stop()
  }
}

