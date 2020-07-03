package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  /*
    - RDDs are essentially distributed typed collections of JVM objects
      - what's the diff between these and datasets?
      - difference: RDD is underlying "first citizen" of spark: all higher-level APIs reduce to RDDs
      - e.g. RDDs are the ones that are actually partitionned
    - Pros: can be highly optimized
        - we can control how they are partitionned
        - order of elements can be controlled
        - order of operations matters for performance
        - advanced spark users can get huge gains from using RDDs
    - Cons: hard to work with
        - for complex operations, need to know the internals of Spark
        - poor APIs for quick data processing
    - for most operations, use DataFrameDataset APIs (spark usually smart enough to optimize code)
    - only use RDDs if you really, really need to care about performance
    - Comparison: RDD vs Dataset
      - in common
        - distributed collections of JVM objects; so we have collection API (map, flatMap, filter, reduce, etc)
        - have combination capability (union), counting (count), and distinct values (distinct)
        - groupBy, sortBy
      - RDDs but not dataset
        - greater partition control: repartition, coalesce, partitioner, zipPartitions, mapPartitions
        - operation control: checkpoint, isCheckpointed, localCheckpoint, cache
        - storage control: cache, getStorageLevel, persist
      - Datasets over RDDs
        - select and join
        - Spark planning/optimization before running code
        - partition control + operation control + storage control has been mostly added by 3.0.0
   */
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // this is how you filter out header; pretty painful
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  // need to convert from a dataset to an RDD to keep type information
  // if you go DF -> RDD you will have RDD[Row], so you'd be losing type information
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info

  // Transformations; can transform RDD like any other collection

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max (actions)
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  // there are more advanced overloaded grouping functions that choose how to partition output data
  // there are also more efficient grouping methods than the groupBy function
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
    Repartitioning is EXPENSIVE. Involves Shuffling.
    Best practice: partition EARLY, then process that.
    Size of a partition 10-100MB.
   */

  // coalesce; to fewer partitions than it currently has
  // we will have 15 partitions, and all other partitions will move data over to these 15 (not a true shuffle)
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // must get member names to match with column names
  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}

/*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+

  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */