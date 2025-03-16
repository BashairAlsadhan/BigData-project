import org.apache.spark.sql.SparkSession

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("Student Stress Analysis")
  .master("local[*]") // Use all available cores
  .getOrCreate()

// Load the file into an RDD
val filePath = "C:\\Users\\basha\\Documents\\GitHub\\BigData-project\\cleaned_student_stress_factors.csv"
val rawRDD = spark.sparkContext.textFile(filePath)

// Extract the header
val header = rawRDD.first()

// Filter out the header and process the data
val dataRDD = rawRDD.filter(row => row != header).map(_.split(","))

// Perform analysis
val highAnxietyStressRDD = dataRDD.filter(arr => arr(0).toDouble >= 0.7 && arr(20).toInt >= 2)
val sleepQualityRDD = dataRDD.map(arr => (arr(20).toInt, arr(7).toDouble))
val avgSleepQuality = sleepQualityRDD.groupByKey().mapValues(values => values.sum / values.size)
val studyLoadRDD = dataRDD.map(arr => (arr(14).toDouble, arr(20).toInt))
val avgStressPerStudyLoad = studyLoadRDD.groupByKey().mapValues(values => values.sum.toDouble / values.size)
val extremeCasesRDD = dataRDD.filter(arr => arr(0).toDouble > 0.8 && arr(3).toDouble > 0.8)
val extremeCount = extremeCasesRDD.count()
val socialSupportRDD = dataRDD.map(arr => (arr(17).toDouble, arr(20).toInt))
val avgStressBySocialSupport = socialSupportRDD.groupByKey().mapValues(values => values.sum.toDouble / values.size)
val stressCombinationRDD = dataRDD.map(arr => (arr(0), arr(3), arr(20))).map {
  case (anxiety, depression, stress) => ((anxiety, depression, stress), 1)
}
val mostCommonStressCombo = stressCombinationRDD.reduceByKey(_ + _).sortBy(_._2, ascending = false)
val highAnxietyCount = dataRDD.filter(arr => arr(0).toDouble >= 0.7).count()
val avgStress = dataRDD.map(arr => arr(20).toInt).sum().toDouble / dataRDD.count()
val worstMentalHealthStudents = extremeCasesRDD.take(5)
val academicStressRDD = dataRDD.map(arr => (arr(13).toDouble, arr(20).toInt))
val avgStressByPerformance = academicStressRDD.groupByKey().mapValues(values => values.sum.toDouble / values.size)
val topStressCombo = mostCommonStressCombo.first()

// Print results
println(s"High Anxiety Students Count: $highAnxietyCount")
println(f"Average Stress Level: $avgStress%.2f")
println("Students with Worst Mental Health Scores:")
worstMentalHealthStudents.foreach(row => println(row.mkString(", ")))
println("Average Stress Level by Academic Performance:")
avgStressByPerformance.collect().foreach(println)
println(s"Most Common Stress Factor Combination: ${topStressCombo._1} (Count: ${topStressCombo._2})")

// Stop SparkSession
spark.stop()