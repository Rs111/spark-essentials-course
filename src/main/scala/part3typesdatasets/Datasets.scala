package part3typesdatasets

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

/*
  Datasets are essentially typed dataframes
    - dataframes themselves are distributed collections of rows
    - datasets are distributed collections of JVM objects (as opposed to dataframes, which are distributed collections of untyped rows)
    - useful when we want to maintain type information or when we want to have the API
      - e.g. when logic we want to express something that is hard to write in Datarame API or SQL
    - Cons
      - performance is less than DFs; all DS transformations are plain scala objects that will be evaluated at runtime
      - Dataframes still have better performance
    - Note: DataFrame is an alias for Dataset[Row]
 */
object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // df has one column, which is an int
  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // we may want to do more than DataFrame operations, given that it is a number; e.g. numbersDF.filter(_ > 40)
  // spark allows us to add more type information to a dataframe; i.e. transforming it to a dataset
  // convert a DF to a Dataset; becomes a distributed collection of ints
  implicit val intEncoder = Encoders.scalaInt // Encoder has the ability to turn Row of dataframe into an Int
  val numbersDS: Dataset[Int] = numbersDF.as[Int] // takes the implicit encoder

  // can now do this
  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // 1 - define your case class; fields in CC need to have same name as JSON file
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double], // need to make it optional if we expect nulls, otherwise we'll get NPE at runtime
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  // 3 - define an encoder (importing the implicits)
  // Encoders.product will take as a type arguement any type that extends the product type; all case classes extend the product type
  // can also just import spart implicits though
  // implicit val carEncoder = Encoders.product[Car]
  import spark.implicits._ // when you import spark implicits, you import all encoders that you might ever want to use
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase()) // turning DataSet[Car] => DataSet[String]

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1
  val carsCount = carsDS.count
  println(carsCount)

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower")))


  // Joins
  // can do `type` if your file contains a variable called type (type is reserved keyword in scala)
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  // joinWith gets you a dataset; if you call join on a dataset, you will lose type information
  // basically like a list1.zip(list2)
  // comes out weird; each dataset is it's own column
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  guitarPlayersDS
    // guitar_id `guitarsDS.col("id")` is contained in array column `guitarPlayersDS.col("guitars")`
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS

  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
