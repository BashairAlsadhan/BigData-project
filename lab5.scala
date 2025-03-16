import org.apache.spark.sql.SparkSession

// Step 1: Initialize SparkSession
val spark = SparkSession.builder.appName("MoviesRDD").master("local[*]").getOrCreate()
val sc = spark.sparkContext

// Step 2: Define the case class
case class Movie(year: Int, title: String, rating: Double)

// Step 3: Read the text file into an RDD
val filePath = "C:/Users/basha/Documents/GitHub/BigData-project/lab5bigdata.csv"  // Ensure this is the correct file path
val moviesRDD = sc.textFile(filePath)

// Step 4: Parse the file correctly into an RDD of Movie objects
val parsedMoviesRDD = moviesRDD.map { line =>
  val parts = line.split(",").map(_.trim) // Split by comma and trim spaces
  Movie(parts(0).toInt, parts(1), parts(2).toDouble)  // Convert fields properly
}

// Ensure parsing is correct
parsedMoviesRDD.take(5).foreach(println)

// 1. Count the number of movies before the year 2000
val moviesBefore2000 = parsedMoviesRDD.filter(_.year < 2000).count()
println(s"Number of movies before 2000: $moviesBefore2000")

// 2. Round up all ratings
val roundedMoviesRDD = parsedMoviesRDD.map(movie => movie.copy(rating = math.ceil(movie.rating)))
println("Movies with rounded-up ratings:")
roundedMoviesRDD.collect().foreach(println)

// 3. Get the newest movie in the dataset
val newestMovie = parsedMoviesRDD.reduce((a, b) => if (a.year > b.year) a else b)
println(s"Newest movie: ${newestMovie}")

// 4. Get the lowest rated movie
val lowestRatedMovie = parsedMoviesRDD.reduce((a, b) => if (a.rating < b.rating) a else b)
println(s"Lowest rated movie: ${lowestRatedMovie}")

// 5. Order the movies in ascending order by year
val sortedMoviesRDD = parsedMoviesRDD.sortBy(_.year)
println("Movies sorted by year (ascending):")
sortedMoviesRDD.collect().foreach(println)

// Stop SparkSession
spark.stop()
