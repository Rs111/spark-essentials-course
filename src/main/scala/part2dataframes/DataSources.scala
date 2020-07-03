package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsSchemaTwo = new StructType()
    .add(StructField("Name", StringType))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    // mode decides what spark should do if it encounters a malformed record
    // permissive likely turns it to null; i.e. if spark fails parsing it will put null
    .option("mode", "failFast") // other options: dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json") // tricky
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
    - what should we do if file we're writing to already exists
   - path
   - zero or more options

   Writing DF creates a folder with a bunch of files
    - the actual data is the .json file in the folder
    - the rest is some metadata
    - crc files validate integrity
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    // can specify the date option if schema has a date-type (only works with an enforced schema)
    // need to specify date format if parsing a date that is not in standard ISO format
    // e.g. if you have a date in your data that's YYYY-MM-dd and you want to call it a date in your schema, need to specify the date format
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags (has the most flags because it's a flexible format)
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "") // parse this value as null in resulting dataframe (null doesn't exist in csv)
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  // parquet is an open source compressed binary data storage format optimized for fast reading of columns
  // works very very well with spark; it is the default storage format for DFs (don't need to specify format)
  // it's very predictable; don't need all the options you have on CSVs
  // it's very small size; way smaller than the JSON file (6-10x times smaller)
  carsDF.write
    //.format()
    .mode(SaveMode.Overwrite)
 //   .parquet()
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver) // class name that will act as the driver for connecting the jdbc
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DF
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
