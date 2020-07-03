package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/*
    - Spark is the most popular unified computing engine & set of libraries for distributed data processing
    - Big data = cannot fit on a standard computer; need a cluster of computers
    - Unified Computing Engine
      - spark supports a variety of data processing/computing tasks
        - data loading
        - SQL queries
        - ML
        - streaming
      - Unified in the sense that spark has some consistent and composable APIs & supports multiple languages
      - Also unified in sense that spark can do optimizations across different libraries
        - e.g. if doing Spark SQL + ML code, spark smart enought to optimize that code across both libraries
      - Computing engine
        - spark is completely detached from data storage and I/O (i.e. from where data resides and how it's being fetched)
      - Libraries
        - Spark SQL, MLlib, etc
        - other open source libraries
      - Context of Big Data
        - Computing vs data
          - CPUs are not getting much faster; approaching limits
          - Data storage keeps getting better and cheaper
          - Gathering data keeps gettign easier/cheaper and more important for companies
        - Data needs to be distributed and processed in parallel because compute cannot keep up with storage + amount of data
        - Standard single-CPU software cannot scale up, leading to the birth of Spark
        - History
          - 2009 UC berkely project by Matei Zaharia
            - Mapreduce was king at this time, but was inefficient for large multi-step applications & ML
            - Needed a seperate application for each step
          - Spark phase 1
            - a simple functional programming API on top of data transformations
            - optimized for multistep applications
            - designed for in-memory computation and data sharing across nodes (shuffles)
          - Spark phase 2
            - interactive data science and adhoc computation
            - Spark shell and spark SQL created
          - Spark phase 3
            - same engine, new libraries (e.g. ML, streaming, GraphX)
        - Spark Misconceptions
          - Spark is not concerned with data sources (it can pull from anywhere; spark is concerned with computing)
          - Spark is not part of Hadoop
            - Spark has nothing to do with hadoop; it interacts very well with Hadoop and HDFS, but it is it's own thing
        - High Level Spark Architecture
          - Low level API
            - Spark has some primitives that allow it to process data at scale
            - RDD is the most important concept; there are also Distributed variables
          - High Level API
            - DataFrames, Datasets, Spark SQL
          - Applications
            - Streaming, ML, GraphX, etc

   */

object DataFramesBasics extends App {

  /* DF is a schema + distributed collection of rows that conform to the schema
    - each node gets some rows + schema
    - Note:
      - Schema types are known to spark when DF is being used, not at compile time
      - Datasets are however typesafe
    - schema can hold arbitrary number of columns
    - spark uses partitionning to split data
      - partitioning splits the data into files, distributes between nodes in the cluster
      - partitionning impacts parallelism
    - dataframes are immutable
    - transformations: narrow vs narrow
    - shuffle = data exchange between cluster nodes in spark cluster
      - e.g. one or more input partitions contribute to one or more output partitions
      - occur in wide transformations
      - big performance topic
      - often best to do data aggregations/joins at the end of your app if you can; e.g. do all filters first
    - lazy evaluation
      - spark waits until action to execute any code
    - planning
      - spark compiles all the DF transformations into a graph before running any code (incl data transfers / shuffles)
      - two plans that spark compiles
        - logical plan = DF dependency graph + narrow/wide transformations sequence that spark will have to execute
        - physical plan = optimize the sequence of steps for nodes in the cluster (i.e. it will plan which nodes will do what)
      - because spark knows all steps in advance, it's able to insert optimizations into the plans
    - transformactions vs actions
      - transformations describe how new DFs are obtained
      - actions actually start executing spark code
   */

  // creating a SparkSession
  // spark session is entrypoint for creating/writing/reading dataframes
  val spark = SparkSession.builder()
    .appName("DataFrames Basics") // shows up in Spark UI
    .config("spark.master", "local") // for now we do local
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read // creates dataframe reader
    .format("json")
    .option("inferSchema", "true") // bad production setting; don't use in practice
    .load("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  // get array of rows
  firstDF.take(10)//.foreach(println)

  // spark types (case objects)
  // types not known at compile time, but known at runtime when spark evaluates the dataset
  val longType = LongType

  // schema
  // note: nullable=false is not a constraint; they are just a marker for spart to optimize for nulls
  // you're basically saying "hey spark, I'm pretty sure this column has no nulls"; but if they do, it can lead to errors/exceptions
  // when we inferSchema, spark always has nullable=true
  // only use nullable=false if you're really sure columns can't be null
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain a schema
  val carsDFSchema = firstDF.schema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred due to tuple types being known

  // note: DFs have schemas, rows do not

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")


  /**
    * Exercise:
    * 1) Create a manual DF describing smartphones
    *   - make
    *   - model
    *   - screen dimension
    *   - camera megapixels
    *
    * 2) Read another file from the data/ folder, e.g. movies.json
    *   - print its schema
    *   - count the number of rows, call count()
    */

  // 1
  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0)
  )

  val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
  smartphonesDF.show()

  // 2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The Movies DF has ${moviesDF.count()} rows")
}
